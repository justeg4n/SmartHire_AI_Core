# =============================================================================
# text_processor.py — SmartHire AI | Text Processing Lambda
#
# Responsibility: Extract text → Mask PII → Claude Analysis → Interview Guide
#
# Architecture role: First half of processing pipeline
#   IngestionTrigger → text_processor → vector_ops
#   Future: Step Functions Task State
# =============================================================================

import json
import math
import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.config import Config


# ---------------------------------------------------------------------------
# AWS Clients
# ---------------------------------------------------------------------------

retry_config = Config(
    region_name="ap-southeast-1",
    retries={"max_attempts": 10, "mode": "adaptive"},
)

bedrock = boto3.client("bedrock-runtime", config=retry_config)
textract = boto3.client("textract", region_name="ap-southeast-1")
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
lambda_client = boto3.client("lambda", config=retry_config)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAUDE_MODEL_ID = os.environ.get("CLAUDE_MODEL_ID") or os.environ.get(
    "BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0"
)
DYNAMODB_TABLE = (
    os.environ.get("DYNAMODB_TABLE_NAME")
    or os.environ.get("DYNAMODB_TABLE")
    or "SmartHire_Profiles"
)
PARSE_STATUS_TTL_DAYS = int(os.environ.get("PARSE_STATUS_TTL_DAYS", "30"))

VECTOR_OPS_FUNCTION = os.environ.get("VECTOR_OPS_FUNCTION_NAME", "")
INVOKE_VECTOR_OPS = os.environ.get("INVOKE_VECTOR_OPS", "false").lower() == "true"

# Feature flags
ENABLE_BLIND_SCREENING = os.environ.get("ENABLE_BLIND_SCREENING", "true").lower() == "true"
ENABLE_INTERVIEW_GUIDE = os.environ.get("ENABLE_INTERVIEW_GUIDE", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Utility Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def expires_at_epoch() -> int:
    return int(
        (datetime.now(timezone.utc) + timedelta(days=PARSE_STATUS_TTL_DAYS)).timestamp()
    )


def safe_json_parse(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        return {
            "summary": "Model output was not valid JSON",
            "rawModelOutput": text,
            "frontend_skills": [],
            "backend_skills": [],
            "devops_skills": [],
            "soft_skills": [],
            "years_experience": 0,
            "matching_score": 0,
            "strengths": "",
            "gaps": "",
        }


# ---------------------------------------------------------------------------
# Textract: CV Text Extraction
# ---------------------------------------------------------------------------

def extract_text_from_s3(bucket: str, key: str) -> str:
    """Extract text from PDF using Amazon Textract (async)."""
    response = textract.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}}
    )
    job_id = response["JobId"]

    while True:
        result = textract.get_document_text_detection(JobId=job_id, MaxResults=1000)
        status = result.get("JobStatus")
        if status == "SUCCEEDED":
            break
        if status == "FAILED":
            raise RuntimeError("Textract job failed")
        time.sleep(2)

    lines = []
    next_token = None
    while True:
        kwargs = {"JobId": job_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        page = textract.get_document_text_detection(**kwargs)
        for block in page.get("Blocks", []):
            if block.get("BlockType") == "LINE" and block.get("Text"):
                lines.append(block["Text"])
        next_token = page.get("NextToken")
        if not next_token:
            break

    return " ".join(lines)


# ---------------------------------------------------------------------------
# Blind Screening — spaCy NER PII Masker
# ---------------------------------------------------------------------------

_spacy_nlp = None

_PII_LABEL_TOKENS = {
    "PERSON": "[CANDIDATE_NAME]",
    "ORG":    "[ORGANIZATION]",
    "GPE":    "[LOCATION]",
    "LOC":    "[LOCATION]",
    "NORP":   "[DEMOGRAPHIC]",
}


def _get_spacy_model():
    """Load and cache spaCy NER model. Non-fatal if unavailable."""
    global _spacy_nlp

    if _spacy_nlp is not None:
        return _spacy_nlp

    try:
        import spacy  # type: ignore

        baked_model_path = os.environ.get("SPACY_MODEL_PATH", "/opt/ml/spacy_model")
        if os.path.isdir(baked_model_path):
            _spacy_nlp = spacy.load(baked_model_path)
            print(f"INFO: spaCy model loaded from: {baked_model_path}")
        else:
            _spacy_nlp = spacy.load("en_core_web_sm")
            print("INFO: spaCy model loaded from package.")
        return _spacy_nlp

    except (ImportError, OSError) as exc:
        print(f"WARNING: spaCy unavailable: {exc}. Blind screening disabled.")
        return None


def mask_pii_entities(raw_text: str) -> tuple[str, dict]:
    """Replace PII entities with neutral tokens. Non-fatal fallback."""
    if not ENABLE_BLIND_SCREENING:
        return raw_text, {"blind_screening": "disabled"}

    nlp = _get_spacy_model()
    if nlp is None:
        return raw_text, {"blind_screening": "unavailable"}

    doc = nlp(raw_text)
    entities_to_mask = [
        ent for ent in doc.ents if ent.label_ in _PII_LABEL_TOKENS
    ]
    entities_to_mask.sort(key=lambda e: e.start_char, reverse=True)

    masked_text = raw_text
    masking_counts: dict[str, int] = {}

    for ent in entities_to_mask:
        label = ent.label_
        token = _PII_LABEL_TOKENS[label]
        masked_text = masked_text[: ent.start_char] + token + masked_text[ent.end_char:]
        masking_counts[label] = masking_counts.get(label, 0) + 1

    total_masked = sum(masking_counts.values())
    masking_report = {
        "blind_screening": "applied",
        "total_entities_masked": total_masked,
        "by_label": masking_counts,
    }
    print(f"INFO: Masked {total_masked} PII entities: {masking_counts}")
    return masked_text, masking_report


# ---------------------------------------------------------------------------
# Claude Agent 1: CV Skill Extraction & Scoring
# ---------------------------------------------------------------------------

def parse_and_evaluate_cv(
    masked_cv_text: str,
    jd_text: str,
    job_title: str,
    required_skills: list[str],
    match_score_placeholder: float = 0.0,
) -> dict:
    """
    Claude agent: extract structured skills and write strengths/gaps.

    Note: match_score_placeholder is 0 at this stage. The real score will
    be computed by vector_ops and merged later. Claude focuses on skill
    extraction and qualitative analysis.
    """
    if not jd_text or jd_text == "Evaluate general technical skills.":
        system_prompt = f"""You are an elite Senior Technical Recruiter AI.

You are performing a general evaluation of a candidate's CV to extract their skills and seniority.

Note: Candidate names, organizations, and locations are anonymized with tokens
(e.g., [CANDIDATE_NAME], [ORGANIZATION]). Evaluate based on technical merit only.

Extract skills and write a professional technical summary.

Output ONLY a valid JSON object. No markdown, no conversational text.

JSON Schema:
{{
  "seniority_estimate": "Junior/Mid/Senior",
  "frontend_skills": ["array of strings"],
  "backend_skills": ["array of strings"],
  "devops_skills": ["array of strings"],
  "soft_skills": ["array of strings"],
  "years_experience": 0,
  "matching_score": 0,
  "strengths": "General technical strengths.",
  "gaps": "Areas for growth based on seniority."
}}"""
        user_message = f"<resume>\n{masked_cv_text[:120000]}\n</resume>"
    else:
        system_prompt = f"""You are an elite Senior Technical Recruiter AI.

You are evaluating a candidate for the role: "{job_title}".
Primary required skills: {", ".join(required_skills) if required_skills else "Derive from JD text"}.

Note: Candidate names, organizations, and locations are anonymized with tokens
(e.g., [CANDIDATE_NAME], [ORGANIZATION]). Evaluate based on technical merit only.

Extract skills and write a professional summary of Strengths and Gaps for the "{job_title}" role.

Output ONLY a valid JSON object. No markdown, no conversational text.

JSON Schema:
{{
  "seniority_estimate": "Junior/Mid/Senior",
  "frontend_skills": ["array of strings"],
  "backend_skills": ["array of strings"],
  "devops_skills": ["array of strings"],
  "soft_skills": ["array of strings"],
  "years_experience": 0,
  "matching_score": 0,
  "strengths": "Short paragraph explaining why they are a fit for {job_title}.",
  "gaps": "Short paragraph explaining what required skills they are missing."
}}"""
        user_message = (
            f"<job_description>\n{jd_text}\n</job_description>\n"
            f"<resume>\n{masked_cv_text[:120000]}\n</resume>"
        )
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "temperature": 0.0,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}],
    }
    response = bedrock.invoke_model(
        modelId=CLAUDE_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    response_data = json.loads(response.get("body").read())
    model_text = response_data.get("content", [{}])[0].get("text", "{}")
    return safe_json_parse(model_text)


# ---------------------------------------------------------------------------
# Claude Agent 2: Interview Guide Generation
# ---------------------------------------------------------------------------

def generate_interview_guide(
    parsed_result: dict,
    job_title: str,
    required_skills: list[str],
) -> list[dict]:
    """Generate 3 targeted interview questions from gaps. Non-fatal."""
    if not ENABLE_INTERVIEW_GUIDE:
        return []

    gaps = parsed_result.get("gaps", "")
    seniority = parsed_result.get("seniority_estimate", "Unknown")
    years_exp = parsed_result.get("years_experience", 0)
    all_claimed = (
        parsed_result.get("backend_skills", [])
        + parsed_result.get("frontend_skills", [])
        + parsed_result.get("devops_skills", [])
    )

    system_prompt = f"""You are a Senior Technical Interviewer AI for {job_title} roles.

A {seniority} candidate with {years_exp} year(s) of experience has been evaluated.

Generate exactly 3 specific technical interview questions targeting their weaknesses.
Each question must have a clear, verifiable correct answer.

Output ONLY a valid JSON array. No markdown.

Schema:
[
  {{
    "question": "Specific technical question",
    "skill_targeted": "Exact skill or gap being tested",
    "rationale": "What a wrong answer reveals"
  }}
]"""

    user_message = f"""Profile:
- Role: {job_title}, Seniority: {seniority}, Experience: {years_exp}y
- Skills: {", ".join(all_claimed) if all_claimed else "Not specified"}
- Required: {", ".join(required_skills) if required_skills else "See gaps"}
- Gaps: {gaps if gaps else "No major gaps identified"}"""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1200,
        "temperature": 0.3,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}],
    }

    try:
        response = bedrock.invoke_model(
            modelId=CLAUDE_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload),
        )
        response_data = json.loads(response.get("body").read())
        model_text = response_data.get("content", [{}])[0].get("text", "[]")

        cleaned = model_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
        cleaned = cleaned.strip()

        questions = json.loads(cleaned)
        if isinstance(questions, list) and all(
            isinstance(q, dict) and "question" in q for q in questions
        ):
            print(f"INFO: Interview guide: {len(questions)} question(s).")
            return questions[:3]
        return []

    except Exception as exc:
        print(f"WARNING: Interview guide failed (non-fatal): {exc}")
        return []


# ---------------------------------------------------------------------------
# DynamoDB: Save processing failure
# ---------------------------------------------------------------------------

def save_failure_to_dynamodb(
    profile_id: str, file_key: str, error_message: str
) -> None:
    """Save a FAILED status to DynamoDB for visibility."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    now_str = now_iso()
    table.put_item(Item={
        "PK": f"CANDIDATE#{profile_id}",
        "SK": f"CV#{file_key}",
        "GSI1PK": "CANDIDATE",
        "GSI1SK": now_str,
        "candidateId": str(profile_id),
        "objectKey": str(file_key),
        "type": "CV_PARSE_FAILURE",
        "parseStatus": "FAILED",
        "updatedAt": now_str,
        "expiresAt": expires_at_epoch(),
        "parsedResult": {},
        "errorMessage": error_message,
    })


# ---------------------------------------------------------------------------
# Processing Router → vector_ops
# ---------------------------------------------------------------------------

def invoke_vector_ops(payload: dict) -> dict:
    """Forward processing results to vector_ops Lambda."""
    if not VECTOR_OPS_FUNCTION:
        raise ValueError("VECTOR_OPS_FUNCTION_NAME not configured.")

    print(f"INFO: Invoking vector_ops for profile={payload['profile_id']}")

    response = lambda_client.invoke(
        FunctionName=VECTOR_OPS_FUNCTION,
        InvocationType="Event",  # Async
        Payload=json.dumps(payload, default=str),
    )

    status_code = response.get("StatusCode", 0)
    if status_code not in (200, 202):
        raise RuntimeError(f"vector_ops invocation failed: HTTP {status_code}")

    print(f"INFO: vector_ops invoked successfully (async, HTTP {status_code})")
    return {"invoked": True, "status_code": status_code}


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Process a single CV: Textract → PII mask → Claude → forward to vector_ops.

    Input: payload dict from ingestion_trigger or Step Functions.
    """
    profile_id = event.get("profile_id", "unknown")
    file_key = event.get("file_key", "unknown")

    try:
        bucket = event.get("bucket", "")
        job_id = event.get("job_id", "unknown")
        jd_text = event.get("jd_text", "Evaluate general technical skills.")
        job_title = event.get("job_title", "Unknown")
        required_skills = event.get("required_skills", [])
        jd_vector = event.get("jd_vector")

        if not bucket:
            raise ValueError("Missing bucket name")
        if profile_id == "unknown":
            raise ValueError("Cannot derive candidate profile id")

        print(f"INFO: text_processor — profile={profile_id}, job={job_id}, key={file_key}")

        # ── [1] Textract: extract raw CV text ────────────────────────
        raw_cv_text = extract_text_from_s3(bucket, file_key)
        print(f"INFO: Textract complete — {len(raw_cv_text)} chars extracted")

        # ── [2] Blind Screening: mask PII ────────────────────────────
        print("INFO: Running blind screening (PII masking).")
        masked_cv_text, masking_report = mask_pii_entities(raw_cv_text)

        # ── [3] Claude Agent 1: skill extraction ─────────────────────
        print("INFO: Running Claude Agent 1 (skill extraction).")
        parsed_data = parse_and_evaluate_cv(
            masked_cv_text, jd_text, job_title, required_skills
        )

        # ── [4] Claude Agent 2: interview guide ──────────────────────
        print("INFO: Running Claude Agent 2 (interview questions).")
        interview_guide = generate_interview_guide(
            parsed_data, job_title, required_skills
        )

        # ── [5] Build payload for vector stage ───────────────────────
        vector_ops_payload = {
            "profile_id": profile_id,
            "job_id": job_id,
            "file_key": file_key,
            "bucket": bucket,
            "jd_text": jd_text,
            "job_title": job_title,
            "required_skills": required_skills,
            "jd_vector": jd_vector,
            "masked_cv_text": masked_cv_text[:50000],  # Truncate for payload limits
            "parsed_data": parsed_data,
            "interview_guide": interview_guide,
            "masking_report": masking_report,
        }

        if INVOKE_VECTOR_OPS:
            invoke_vector_ops(vector_ops_payload)
            return {
                "statusCode": 200,
                "body": f"text_processor complete for {profile_id}",
            }

        return {
            "statusCode": 200,
            "payload": vector_ops_payload,
        }

    except Exception as exc:
        print(json.dumps({
            "level": "error",
            "message": "text_processor failed",
            "profileId": profile_id,
            "fileKey": file_key,
            "error": str(exc),
        }))
        if profile_id != "unknown" and file_key != "unknown":
            save_failure_to_dynamodb(profile_id, file_key, str(exc))
        raise
