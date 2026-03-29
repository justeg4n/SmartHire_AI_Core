# =============================================================================
# report_generator_lambda.py — SmartHire AI | Interview Report Generator
#
# Responsibility: Receive aggregated STAR scores + emotion metrics + code
# results from Step Functions, generate a narrative executive summary via
# Bedrock Claude, persist the full report to DynamoDB and S3, then push a
# real-time notification to the recruiter's dashboard via AppSync.
#
# INVOCATION: Step Functions State 4 (final AI state) in the report pipeline.
# State 3 (Composite, a pure Step Functions Pass state) computes weighted
# scores without invoking any Lambda — it uses Step Functions intrinsic
# functions and passes the result here.
#
# WHY PERSIST REPORT TO BOTH DYNAMODB AND S3:
#   - DynamoDB (hot store): immediate availability for the recruiter dashboard
#     to query via GET /api/interviews/{sessionId}/report. Low-latency reads.
#   - S3 (cold store): immutable audit trail. Hiring decisions can be legally
#     challenged; the original report must be preserved unchanged. The S3 key
#     includes a timestamp so prior reports are not overwritten on re-runs.
#
# WHY APPSYNC NOTIFICATION:
#   The recruiter is watching the interview dashboard. Without a push, they
#   must manually refresh or poll. AppSync subscription delivers the report
#   summary within milliseconds of generation, enabling instant review.
#
# INPUT EVENT (from Step Functions State 3 Composite):
# {
#   "session_id":         "uuid",
#   "star_scores":        [{turn_number, composite_score, ...}, ...],
#   "overall_star_score": 74.3,
#   "emotion_metrics":    { percent_time_calm, average_attention_score, ... },
#   "code_results":       { passed: 7, failed: 1, runtime_ms: 142 }
# }
#
# RETURN (Step Functions State 4 output → State 5 Persist, if separate):
# {
#   "statusCode":      200,
#   "session_id":      "uuid",
#   "report_summary":  "300-word narrative...",
#   "hire_recommendation": "HIRE" | "NO_HIRE" | "MAYBE",
#   "overall_score":   74.3,
#   "ddb_sk":          "REPORT#2024-01-15T10:30:00Z",
#   "s3_key":          "reports/uuid/report-2024-01-15T10:30:00Z.json"
# }
# =============================================================================

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import urllib.request
import urllib.error
import hashlib
import hmac
from botocore.config import Config
from botocore.exceptions import ClientError
from aws_xray_sdk.core import patch_all, xray_recorder

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cold-start singletons
# ---------------------------------------------------------------------------

_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
_retry  = Config(region_name=_REGION, retries={"max_attempts": 3, "mode": "adaptive"})

bedrock_client = boto3.client("bedrock-runtime", config=_retry)
dynamodb       = boto3.resource("dynamodb", region_name=_REGION)
s3_client      = boto3.client("s3",         config=_retry)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MODEL_ID       = os.environ.get("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME", "SmartHire_ApplicationTracking")
REPORT_BUCKET  = os.environ.get("REPORT_BUCKET", "")
APPSYNC_URL    = os.environ.get("APPSYNC_URL", "")
APPSYNC_REGION = os.environ.get("APPSYNC_REGION", _REGION)

# Score thresholds for HIRE recommendation
HIRE_THRESHOLD    = float(os.environ.get("HIRE_THRESHOLD",    "70.0"))
MAYBE_THRESHOLD   = float(os.environ.get("MAYBE_THRESHOLD",   "55.0"))


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_decimal(obj):
    """Recursively convert floats to Decimal for DynamoDB storage."""
    return json.loads(json.dumps(obj), parse_float=Decimal)


# ---------------------------------------------------------------------------
# Step 1: Generate narrative with Bedrock Claude
# ---------------------------------------------------------------------------

@xray_recorder.capture("GenerateNarrative")
def _generate_narrative(star_scores: list, emotion_metrics: dict,
                         code_results: dict, overall_score: float) -> tuple[str, str]:
    """
    Write a 300-word executive summary and determine HIRE recommendation.

    The prompt separates the data into three clearly labelled sections so
    Claude can draw on all three dimensions rather than overweighting whichever
    appears first. Temperature=0.2 keeps the output factual and consistent —
    reports should read the same way across candidates at the same score level.

    Returns (summary_text, hire_recommendation).
    """
    system_prompt = """You are an Expert Technical Hiring Manager writing a structured interview report for SmartHire.

Given the quantitative evaluation data, write a professional 300-word executive summary covering:
1. Technical competency (based on STAR scores)
2. Communication and presentation under pressure (based on emotion metrics)
3. Coding ability and approach (based on code execution results)
4. A final HIRE / MAYBE / NO_HIRE recommendation backed by the data

RULES:
- Base every statement on the numbers provided. Do not invent qualities.
- Be direct and specific. Avoid generic phrases like "showed potential".
- The recommendation must appear as the final line in exactly this format:
  RECOMMENDATION: HIRE  (or MAYBE, or NO_HIRE)
- Output ONLY the narrative text. No JSON, no headers, no markdown."""

    star_summary = [
        {
            "turn":            s.get("turn_number"),
            "composite_score": s.get("star_evaluation", {}).get("composite_score"),
            "justification":   s.get("star_evaluation", {}).get("justification", "")[:200],
        }
        for s in (star_scores or [])
    ]

    user_message = (
        f"<overall_score>{overall_score}/100</overall_score>\n"
        f"<star_evaluation_by_turn>\n{json.dumps(star_summary, indent=2)}\n</star_evaluation_by_turn>\n"
        f"<emotion_metrics>\n{json.dumps(emotion_metrics, indent=2)}\n</emotion_metrics>\n"
        f"<code_results>\n{json.dumps(code_results, indent=2)}\n</code_results>\n"
        f"Please write the executive summary."
    )

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        600,
        "temperature":       0.2,
        "system":            system_prompt,
        "messages":          [{"role": "user", "content": user_message}],
    }

    resp    = bedrock_client.invoke_model(
        modelId=MODEL_ID, contentType="application/json",
        accept="application/json", body=json.dumps(payload),
    )
    summary = json.loads(resp["body"].read())["content"][0]["text"].strip()
    logger.info("Narrative generated.")

    # Extract HIRE recommendation from the last line
    last_line = summary.strip().splitlines()[-1].upper()
    if "HIRE" in last_line and "NO_HIRE" not in last_line:
        recommendation = "HIRE"
    elif "NO_HIRE" in last_line:
        recommendation = "NO_HIRE"
    elif "MAYBE" in last_line:
        recommendation = "MAYBE"
    else:
        # Fallback: derive from numeric score
        if overall_score >= HIRE_THRESHOLD:
            recommendation = "HIRE"
        elif overall_score >= MAYBE_THRESHOLD:
            recommendation = "MAYBE"
        else:
            recommendation = "NO_HIRE"

    return summary, recommendation


# ---------------------------------------------------------------------------
# Step 2: Persist to DynamoDB
# ---------------------------------------------------------------------------

def _save_to_dynamodb(session_id: str, report: dict, sk: str) -> None:
    """
    Write the report to DynamoDB under the session's partition key.

    SK = REPORT#<timestamp> so re-running the pipeline creates a new record
    rather than overwriting the original. The recruiter dashboard queries
    begins_with(SK, "REPORT#") and shows the latest one.
    """
    dynamodb.Table(DYNAMODB_TABLE).put_item(Item={
        "PK":   f"SESSION#{session_id}",
        "SK":   sk,
        "type": "INTERVIEW_REPORT",
        **_to_decimal(report),
    })
    logger.info(f"Report saved to DynamoDB: SK={sk}")


# ---------------------------------------------------------------------------
# Step 3: Save full JSON report to S3
# ---------------------------------------------------------------------------

def _save_to_s3(session_id: str, report: dict, timestamp: str) -> str:
    """
    Save the complete report JSON to S3 for immutable audit trail.

    Key format: reports/{session_id}/report-{timestamp}.json
    Uses ServerSideEncryption="AES256" — no additional KMS costs for
    report storage, but data is encrypted at rest.
    """
    if not REPORT_BUCKET:
        logger.warning("REPORT_BUCKET not set — skipping S3 save.")
        return ""

    key  = f"reports/{session_id}/report-{timestamp}.json"
    body = json.dumps(report, default=str, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=REPORT_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    logger.info(f"Report saved to S3: s3://{REPORT_BUCKET}/{key}")
    return key


# ---------------------------------------------------------------------------
# Step 4: AppSync notification (SigV4 signed)
# ---------------------------------------------------------------------------

def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_headers(url: str, body: str) -> dict:
    """Build Authorization headers for a SigV4-signed AppSync POST request."""
    import time as _time
    access_key    = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key    = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    session_token = os.environ.get("AWS_SESSION_TOKEN", "")

    now       = datetime.utcnow()
    amz_date  = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    service   = "appsync"

    import urllib.parse as _up
    host     = _up.urlparse(url).netloc
    uri      = _up.urlparse(url).path or "/"
    pay_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    can_headers = f"content-type:application/json\nhost:{host}\nx-amz-date:{amz_date}\n"
    signed_hdrs = "content-type;host;x-amz-date"
    if session_token:
        can_headers += f"x-amz-security-token:{session_token}\n"
        signed_hdrs += ";x-amz-security-token"

    can_req   = f"POST\n{uri}\n\n{can_headers}\n{signed_hdrs}\n{pay_hash}"
    cred_scope = f"{date_stamp}/{APPSYNC_REGION}/{service}/aws4_request"
    str_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{cred_scope}\n{hashlib.sha256(can_req.encode()).hexdigest()}"

    k_date    = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region  = _sign(k_date, APPSYNC_REGION)
    k_service = _sign(k_region, service)
    k_sig     = _sign(k_service, "aws4_request")
    signature = hmac.new(k_sig, str_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    auth = (f"AWS4-HMAC-SHA256 Credential={access_key}/{cred_scope}, "
            f"SignedHeaders={signed_hdrs}, Signature={signature}")
    headers = {"Content-Type": "application/json", "x-amz-date": amz_date, "Authorization": auth}
    if session_token:
        headers["x-amz-security-token"] = session_token
    return headers


@xray_recorder.capture("NotifyAppSync")
def _notify_appsync(session_id: str, report_summary: dict) -> bool:
    """
    Push report-ready notification to AppSync so the recruiter's dashboard
    updates in real time without polling.

    GraphQL mutation required in AppSync schema:
      mutation PublishInterviewReport(
        $sessionId: String!, $reportSummary: AWSJSON!, $updatedAt: AWSDateTime!
      ) {
        publishInterviewReport(
          sessionId: $sessionId, reportSummary: $reportSummary, updatedAt: $updatedAt
        ) { sessionId updatedAt }
      }

    Subscription on the recruiter dashboard:
      subscription { onInterviewReport(sessionId: $sessionId) { sessionId reportSummary } }
    """
    if not APPSYNC_URL:
        logger.warning("APPSYNC_URL not configured — skipping notification.")
        return False

    mutation = """
    mutation PublishInterviewReport(
      $sessionId: String! $reportSummary: AWSJSON! $updatedAt: AWSDateTime!
    ) {
      publishInterviewReport(
        sessionId: $sessionId reportSummary: $reportSummary updatedAt: $updatedAt
      ) { sessionId updatedAt }
    }
    """
    variables = {
        "sessionId":     session_id,
        "reportSummary": json.dumps(report_summary),
        "updatedAt":     _now_iso(),
    }
    body    = json.dumps({"query": mutation, "variables": variables})
    headers = _sigv4_headers(APPSYNC_URL, body)

    try:
        req = urllib.request.Request(
            APPSYNC_URL, data=body.encode("utf-8"),
            headers=headers, method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            resp_body = json.loads(r.read().decode("utf-8"))
        if "errors" in resp_body:
            logger.warning(f"AppSync errors: {resp_body['errors']}")
            return False
        logger.info("AppSync notification sent.")
        return True
    except Exception as exc:
        logger.warning(f"AppSync notification failed (non-fatal): {exc}")
        return False


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

@xray_recorder.capture("ReportGenerator")
def lambda_handler(event, context):
    """
    Step Functions State 4 handler: generate and persist the interview report.
    """
    session_id    = event.get("session_id",        "unknown")
    star_scores   = event.get("star_scores",       [])
    overall_score = float(event.get("overall_star_score", 0.0))
    emotion_metrics = event.get("emotion_metrics", {})
    code_results  = event.get("code_results",      {})

    logger.info(f"ReportGenerator: session={session_id} overall_score={overall_score}")

    try:
        # ── [1] Generate narrative ────────────────────────────────────────
        summary, recommendation = _generate_narrative(
            star_scores, emotion_metrics, code_results, overall_score
        )

        now  = _now_iso()
        sk   = f"REPORT#{now}"
        report = {
            "sessionId":           session_id,
            "overallScore":        overall_score,
            "hireRecommendation":  recommendation,
            "executiveSummary":    summary,
            "starScores":          star_scores,
            "emotionMetrics":      emotion_metrics,
            "codeResults":         code_results,
            "generatedAt":         now,
        }

        # ── [2] Persist to DynamoDB ───────────────────────────────────────
        _save_to_dynamodb(session_id, {**report, "PK": f"SESSION#{session_id}", "SK": sk}, sk)

        # ── [3] Save full JSON to S3 ──────────────────────────────────────
        s3_key = _save_to_s3(session_id, report, now)

        # ── [4] Notify recruiter via AppSync ──────────────────────────────
        # Send only the summary fields — the full report is fetched on demand
        _notify_appsync(session_id, {
            "sessionId":          session_id,
            "overallScore":       overall_score,
            "hireRecommendation": recommendation,
            "summaryExcerpt":     summary[:400],
            "reportDdbSk":        sk,
            "reportS3Key":        s3_key,
        })

        logger.info(f"Report complete: {recommendation} ({overall_score}/100)")

        return {
            "statusCode":          200,
            "session_id":          session_id,
            "report_summary":      summary,
            "hire_recommendation": recommendation,
            "overall_score":       overall_score,
            "ddb_sk":              sk,
            "s3_key":              s3_key,
        }

    except ClientError as exc:
        logger.error(f"Bedrock error: {exc}")
        raise  # Step Functions retries

    except Exception as exc:
        logger.error(f"ReportGenerator failed: {exc}", exc_info=True)
        raise
