# =============================================================================
# appsync_notifier.py — SmartHire AI | AppSync Real-Time Notifier (Utility)
#
# Responsibility: Send GraphQL mutations to AWS AppSync to push real-time
# updates to the React frontend via WebSocket subscriptions.
#
# Used by:
#   - job_suggestion_engine.py → notifies candidate of new job suggestions
#   - candidate_ranking_engine.py → notifies recruiter of ranked candidates
#
# Auth: IAM Signature V4 (recommended for Lambda-to-AppSync). The Lambda
# execution role must have appsync:GraphQL permission on the API ARN.
#
# Deployment note: No extra dependencies required — uses only stdlib urllib
# and botocore (already available in any Lambda environment).
#
# AppSync GraphQL Schema required (add to schema.graphql):
#
#   type JobSuggestionUpdate {
#     candidateId: String!
#     suggestions: AWSJSON!
#     updatedAt: AWSDateTime!
#   }
#
#   type CandidateRankingUpdate {
#     jobId: String!
#     rankedCandidates: AWSJSON!
#     updatedAt: AWSDateTime!
#   }
#
#   type Mutation {
#     publishJobSuggestions(
#       candidateId: String!, suggestions: AWSJSON!, updatedAt: AWSDateTime!
#     ): JobSuggestionUpdate
#
#     publishCandidateRanking(
#       jobId: String!, rankedCandidates: AWSJSON!, updatedAt: AWSDateTime!
#     ): CandidateRankingUpdate
#   }
#
#   type Subscription {
#     onJobSuggestions(candidateId: String!): JobSuggestionUpdate
#       @aws_subscribe(mutations: ["publishJobSuggestions"])
#
#     onCandidateRanking(jobId: String!): CandidateRankingUpdate
#       @aws_subscribe(mutations: ["publishCandidateRanking"])
#   }
# =============================================================================

import hashlib
import hmac
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Full AppSync GraphQL endpoint URL.
# Example: https://abc123xyz.appsync-api.ap-southeast-1.amazonaws.com/graphql
APPSYNC_URL = os.environ.get("APPSYNC_URL", "")

# AWS region where AppSync is deployed.
APPSYNC_REGION = os.environ.get("APPSYNC_REGION", "ap-southeast-1")

# Set to "true" to skip AppSync calls entirely (useful in local testing).
APPSYNC_ENABLED = os.environ.get("APPSYNC_ENABLED", "true").lower() == "true"


# ---------------------------------------------------------------------------
# AWS Signature V4 Implementation
# ---------------------------------------------------------------------------
# We implement SigV4 manually using stdlib only.
# This avoids adding `requests` + `requests-aws4auth` as dependencies.
# ---------------------------------------------------------------------------

def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def _build_sigv4_headers(
    url: str,
    body: str,
    region: str,
) -> dict[str, str]:
    """
    Build Authorization + x-amz-date headers for SigV4-signed POST request.

    Reads credentials from the Lambda execution environment automatically
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).
    """
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    session_token = os.environ.get("AWS_SESSION_TOKEN", "")

    service = "appsync"
    method = "POST"

    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc
    canonical_uri = parsed.path or "/"

    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    # Canonical headers (must be sorted alphabetically)
    canonical_headers = (
        f"content-type:application/json\n"
        f"host:{host}\n"
        f"x-amz-date:{amz_date}\n"
    )
    if session_token:
        canonical_headers += f"x-amz-security-token:{session_token}\n"

    signed_headers = "content-type;host;x-amz-date"
    if session_token:
        signed_headers += ";x-amz-security-token"

    canonical_request = "\n".join([
        method,
        canonical_uri,
        "",  # canonical_querystring
        canonical_headers,
        signed_headers,
        payload_hash,
    ])

    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    signing_key = _get_signature_key(secret_key, date_stamp, region, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    headers = {
        "Content-Type": "application/json",
        "x-amz-date": amz_date,
        "Authorization": authorization,
    }
    if session_token:
        headers["x-amz-security-token"] = session_token

    return headers


def _execute_mutation(query: str, variables: dict) -> bool:
    """
    Execute a GraphQL mutation against AppSync with SigV4 auth.

    Returns True on success, False on any failure (non-fatal — callers
    should log but not raise on AppSync failures so the core pipeline
    is never blocked by a notification error).
    """
    if not APPSYNC_ENABLED:
        print("INFO: AppSync notifications disabled (APPSYNC_ENABLED=false).")
        return True

    if not APPSYNC_URL:
        print("WARNING: APPSYNC_URL not set. Real-time notification skipped.")
        return False

    body = json.dumps({"query": query, "variables": variables})

    try:
        headers = _build_sigv4_headers(APPSYNC_URL, body, APPSYNC_REGION)
        req = urllib.request.Request(
            APPSYNC_URL,
            data=body.encode("utf-8"),
            headers=headers,
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=8) as response:
            response_body = json.loads(response.read().decode("utf-8"))

        if "errors" in response_body:
            print(f"WARNING: AppSync mutation returned errors: {response_body['errors']}")
            return False

        print("INFO: AppSync mutation succeeded.")
        return True

    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")[:300]
        print(f"WARNING: AppSync HTTP {exc.code}: {body_text}")
        return False
    except Exception as exc:
        print(f"WARNING: AppSync notification failed (non-fatal): {exc}")
        return False


# ---------------------------------------------------------------------------
# Public Notification Functions
# ---------------------------------------------------------------------------

def notify_candidate_job_suggestions(
    candidate_id: str,
    suggestions: list[dict],
    updated_at: str,
) -> bool:
    """
    Push job suggestion results to a subscribed candidate frontend.

    The React frontend subscribes with:
      subscription { onJobSuggestions(candidateId: $candidateId) { ... } }

    This mutation triggers that subscription for the specific candidate,
    delivering the top matching jobs in real-time without polling.

    Args:
        candidate_id:  The candidate's userId (Cognito sub or DB id).
        suggestions:   List of job match dicts (jobId, jobTitle, matchScore,
                       matchExplanation, requiredSkills).
        updated_at:    ISO 8601 timestamp of when matching was computed.

    Returns:
        True if AppSync accepted the mutation, False otherwise.
    """
    query = """
    mutation PublishJobSuggestions(
      $candidateId: String!
      $suggestions: AWSJSON!
      $updatedAt: AWSDateTime!
    ) {
      publishJobSuggestions(
        candidateId: $candidateId
        suggestions: $suggestions
        updatedAt: $updatedAt
      ) {
        candidateId
        updatedAt
      }
    }
    """
    variables = {
        "candidateId": candidate_id,
        "suggestions": json.dumps(suggestions),
        "updatedAt": updated_at,
    }
    print(f"INFO: Pushing {len(suggestions)} job suggestion(s) to AppSync for candidate {candidate_id}.")
    return _execute_mutation(query, variables)


def notify_recruiter_candidate_ranking(
    job_id: str,
    ranked_candidates: list[dict],
    updated_at: str,
) -> bool:
    """
    Push ranked candidate results to a subscribed recruiter frontend.

    The React frontend (recruiter dashboard) subscribes with:
      subscription { onCandidateRanking(jobId: $jobId) { ... } }

    Args:
        job_id:             The job posting ID.
        ranked_candidates:  List of candidate match dicts (candidateId,
                            seniority, matchScore, snapshot, topSkills).
        updated_at:         ISO 8601 timestamp.

    Returns:
        True if AppSync accepted the mutation, False otherwise.
    """
    query = """
    mutation PublishCandidateRanking(
      $jobId: String!
      $rankedCandidates: AWSJSON!
      $updatedAt: AWSDateTime!
    ) {
      publishCandidateRanking(
        jobId: $jobId
        rankedCandidates: $rankedCandidates
        updatedAt: $updatedAt
      ) {
        jobId
        updatedAt
      }
    }
    """
    variables = {
        "jobId": job_id,
        "rankedCandidates": json.dumps(ranked_candidates),
        "updatedAt": updated_at,
    }
    print(f"INFO: Pushing {len(ranked_candidates)} ranked candidate(s) to AppSync for job {job_id}.")
    return _execute_mutation(query, variables)
