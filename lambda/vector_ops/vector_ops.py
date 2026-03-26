# =============================================================================
# vector_ops.py — SmartHire AI | VectorOps Lambda (Unified Processing)
#
# Responsibility: Extract → PII Mask → Claude Analysis → Embed → Score → Persist
#
# Architecture role: Lambda 2 ("Matcher")
#   IngestionTrigger → VectorOps → (pgvector + DynamoDB + OpenSearch)
#   Future: Step Functions will handle Textract/Claude as SDK tasks,
#           VectorOps will only do Embed + Score + Persist.
# =============================================================================

import json
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

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


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAUDE_MODEL_ID = os.environ.get("CLAUDE_MODEL_ID") or os.environ.get(
    "BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0"
)
COHERE_MODEL_ID = os.environ.get("COHERE_MODEL_ID", "cohere.embed-english-v3")
PARSE_STATUS_TTL_DAYS = int(os.environ.get("PARSE_STATUS_TTL_DAYS", "30"))

# Cross-Encoder
CROSS_ENCODER_MODEL_ID = os.environ.get(
    "CROSS_ENCODER_MODEL_ID", "cross-encoder/ms-marco-MiniLM-L-6-v2"
)
CROSS_ENCODER_BAKED_PATH = os.environ.get(
    "CROSS_ENCODER_MODEL_PATH", "/opt/ml/cross_encoder"
)
CROSS_ENCODER_CHUNK_CHARS = int(os.environ.get("CE_CHUNK_CHARS", "800"))
CROSS_ENCODER_OVERLAP_CHARS = int(os.environ.get("CE_OVERLAP_CHARS", "150"))
CROSS_ENCODER_MAX_CHUNKS = int(os.environ.get("CE_MAX_CHUNKS", "12"))
CROSS_ENCODER_JD_QUERY_CHARS = int(os.environ.get("CE_JD_QUERY_CHARS", "1500"))
CROSS_ENCODER_TOP_K = int(os.environ.get("CE_TOP_K", "3"))

# Hybrid blending weights (must sum to 1.0)
HYBRID_WEIGHT_CROSS = float(os.environ.get("HYBRID_WEIGHT_CROSS", "0.65"))
HYBRID_WEIGHT_BI = float(os.environ.get("HYBRID_WEIGHT_BI", "0.35"))

# Feature flags
ENABLE_BLIND_SCREENING = os.environ.get("ENABLE_BLIND_SCREENING", "true").lower() == "true"
ENABLE_INTERVIEW_GUIDE = os.environ.get("ENABLE_INTERVIEW_GUIDE", "true").lower() == "true"
ENABLE_CROSS_ENCODER = os.environ.get("ENABLE_CROSS_ENCODER", "true").lower() == "true"


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


# ===========================================================================
# PHASE 1: Text Extraction & Analysis
# ===========================================================================


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
) -> dict:
    """Claude agent: extract structured skills and write strengths/gaps."""
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


# ===========================================================================
# PHASE 2: Vector Operations & Scoring
# ===========================================================================


# ---------------------------------------------------------------------------
# Bi-Encoder: Cohere Embeddings + Cosine Similarity
# ---------------------------------------------------------------------------

def get_cohere_embedding(text: str) -> list[float]:
    """Embed text using Cohere embed-v3 via Bedrock. Max 2048 chars."""
    normalized = (text or "").strip()
    safe_text = normalized[:2048] if normalized else "No content"
    payload = {
        "texts": [safe_text],
        "input_type": "search_document",
        "truncate": "END",
    }
    response = bedrock.invoke_model(
        modelId=COHERE_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    response_body = json.loads(response.get("body").read())
    return response_body["embeddings"][0]


def calculate_cosine_similarity(vec1: list[float], vec2: list[float]) -> float:
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = math.sqrt(sum(a * a for a in vec1))
    magnitude2 = math.sqrt(sum(b * b for b in vec2))
    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0
    return dot_product / (magnitude1 * magnitude2)


# ---------------------------------------------------------------------------
# Cross-Encoder Reranking
# ---------------------------------------------------------------------------

_cross_encoder_instance = None


def _sigmoid(x: float) -> float:
    x = max(-500.0, min(500.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def _get_cross_encoder():
    """Load CrossEncoder model. Returns None on failure (non-fatal)."""
    global _cross_encoder_instance

    if _cross_encoder_instance is not None:
        return _cross_encoder_instance

    if not ENABLE_CROSS_ENCODER:
        return None

    try:
        from sentence_transformers import CrossEncoder  # type: ignore

        tmp_cache_path = "/tmp/cross_encoder_model"

        if os.path.isdir(CROSS_ENCODER_BAKED_PATH):
            load_path = CROSS_ENCODER_BAKED_PATH
        elif os.path.isdir(tmp_cache_path):
            load_path = tmp_cache_path
        else:
            os.environ["HF_HOME"] = "/tmp/hf_home"
            temp_model = CrossEncoder(CROSS_ENCODER_MODEL_ID, max_length=512)
            temp_model.save(tmp_cache_path)
            del temp_model
            load_path = tmp_cache_path

        _cross_encoder_instance = CrossEncoder(load_path, max_length=512)
        print("INFO: Cross-encoder ready.")
        return _cross_encoder_instance

    except (ImportError, Exception) as exc:
        print(f"WARNING: Cross-encoder unavailable: {exc}")
        return None


def _create_cv_chunks(cv_text: str) -> list[str]:
    chunks = []
    step = CROSS_ENCODER_CHUNK_CHARS - CROSS_ENCODER_OVERLAP_CHARS
    start = 0
    while start < len(cv_text):
        end = start + CROSS_ENCODER_CHUNK_CHARS
        chunk = cv_text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(cv_text):
            break
        start += step
    return chunks[:CROSS_ENCODER_MAX_CHUNKS]


def _aggregate_chunk_scores(normalized_scores: list[float]) -> float:
    if not normalized_scores:
        return 0.0
    top_k = sorted(normalized_scores, reverse=True)[:CROSS_ENCODER_TOP_K]
    raw_weights = [0.5, 0.3, 0.2][: len(top_k)]
    weight_sum = sum(raw_weights)
    weights = [w / weight_sum for w in raw_weights]
    return sum(score * weight for score, weight in zip(top_k, weights))


def compute_cross_encoder_score(cv_text: str, jd_text: str) -> float | None:
    """Score CV against JD using Cross-Encoder. Returns [0,1] or None."""
    ce = _get_cross_encoder()
    if ce is None:
        return None

    jd_query = jd_text[:CROSS_ENCODER_JD_QUERY_CHARS].strip()
    if not jd_query:
        return None

    cv_chunks = _create_cv_chunks(cv_text)
    if not cv_chunks:
        return None

    print(f"INFO: Cross-encoder scoring {len(cv_chunks)} chunk(s).")
    pairs = [(jd_query, chunk) for chunk in cv_chunks]
    raw_logits = ce.predict(pairs)
    normalized = [_sigmoid(float(logit)) for logit in raw_logits]
    return _aggregate_chunk_scores(normalized)


# ---------------------------------------------------------------------------
# Hybrid Score Computation
# ---------------------------------------------------------------------------

def compute_hybrid_score(
    bi_score_pct: float, cross_score: float | None
) -> tuple[float, dict]:
    """Blend bi-encoder and cross-encoder scores."""
    bi_normalized = bi_score_pct / 100.0

    if cross_score is None:
        return bi_score_pct, {
            "bi_encoder_score": round(bi_score_pct, 2),
            "cross_encoder_score": None,
            "hybrid_score": round(bi_score_pct, 2),
            "method": "bi_encoder_only",
        }

    hybrid = HYBRID_WEIGHT_BI * bi_normalized + HYBRID_WEIGHT_CROSS * cross_score
    final_pct = round(hybrid * 100, 2)
    return final_pct, {
        "bi_encoder_score": round(bi_score_pct, 2),
        "cross_encoder_score": round(cross_score * 100, 2),
        "hybrid_score": final_pct,
        "bi_encoder_weight": HYBRID_WEIGHT_BI,
        "cross_encoder_weight": HYBRID_WEIGHT_CROSS,
        "method": "hybrid_bi_cross_encoder",
    }


# ===========================================================================
# PHASE 3: Persistence
# ===========================================================================


# ---------------------------------------------------------------------------
# pgvector: PostgreSQL Vector Storage (Stage 5)
# ---------------------------------------------------------------------------

def _resolve_rds_credentials() -> dict[str, Any]:
    """Load and cache DB credentials from Secrets Manager."""
    global _rds_secret_cache

    if _rds_secret_cache is not None:
        return _rds_secret_cache

    if not RDS_SECRET_ARN:
        raise RuntimeError("RDS_SECRET_ARN is missing")

    response = secretsmanager.get_secret_value(SecretId=RDS_SECRET_ARN)
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError("RDS secret has no SecretString")

    parsed = json.loads(secret_string)
    _rds_secret_cache = {
        "username": parsed.get("username", ""),
        "password": parsed.get("password", ""),
        "host": parsed.get("host", ""),
        "port": int(parsed.get("port", 5432)),
        "dbname": parsed.get("dbname", RDS_DATABASE),
    }
    return _rds_secret_cache


def _build_vector_literal(values: list[float]) -> str:
    """Serialize Python list into pgvector literal format."""
    return "[" + ",".join(f"{float(v):.8f}" for v in values) + "]"


def _to_user_id(candidate_id: str) -> str | None:
    """Map pipeline candidate_id to UUID user_id expected by SQL schema."""
    try:
        return str(uuid.UUID(str(candidate_id)))
    except (TypeError, ValueError):
        return None


def _upsert_pgvector_with_psycopg2(
    candidate_id: str,
    cv_vector: list[float],
    parsed_data: dict,
    masked_cv_text: str,
) -> bool:
    """Persist profile and embedding using psycopg2."""
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore

    user_id = _to_user_id(candidate_id)
    if user_id is None:
        raise ValueError(f"candidate_id '{candidate_id}' is not a valid UUID user_id")

    secret = _resolve_rds_credentials()
    host = RDS_PROXY_ENDPOINT or secret["host"]
    dbname = RDS_DATABASE or secret["dbname"]
    vector_literal = _build_vector_literal(cv_vector)
    score = float(parsed_data.get("matching_score", 0.0))
    seniority = str(parsed_data.get("seniority_estimate", "Unknown"))[:20]
    years_experience = parsed_data.get("years_experience", 0)
    try:
        years_experience = int(years_experience)
    except (TypeError, ValueError):
        years_experience = 0

    conn = psycopg2.connect(
        host=host,
        port=secret["port"],
        user=secret["username"],
        password=secret["password"],
        dbname=dbname,
        connect_timeout=RDS_CONNECT_TIMEOUT,
        sslmode=RDS_SSL_MODE,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidate_embeddings (
                        user_id,
                        embedding,
                        raw_text
                    )
                    VALUES (%s, %s::vector, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        raw_text = EXCLUDED.raw_text,
                        updated_at = NOW();
                    """
                    ,
                    (user_id, vector_literal, masked_cv_text[:120000]),
                )
                cur.execute(
                    """
                    INSERT INTO candidate_profiles_ai (
                        user_id,
                        extracted_profile,
                        seniority_estimate,
                        years_experience,
                        matching_score,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        extracted_profile = EXCLUDED.extracted_profile,
                        seniority_estimate = EXCLUDED.seniority_estimate,
                        years_experience = EXCLUDED.years_experience,
                        matching_score = EXCLUDED.matching_score,
                        updated_at = NOW();
                    """,
                    (user_id, Json(parsed_data), seniority, years_experience, score),
                )
        return True
    finally:
        conn.close()


def _upsert_pgvector_with_psycopg(
    candidate_id: str,
    cv_vector: list[float],
    parsed_data: dict,
    masked_cv_text: str,
) -> bool:
    """Persist profile and embedding using psycopg v3."""
    import psycopg  # type: ignore
    from psycopg.types.json import Jsonb  # type: ignore

    user_id = _to_user_id(candidate_id)
    if user_id is None:
        raise ValueError(f"candidate_id '{candidate_id}' is not a valid UUID user_id")

    secret = _resolve_rds_credentials()
    host = RDS_PROXY_ENDPOINT or secret["host"]
    dbname = RDS_DATABASE or secret["dbname"]
    vector_literal = _build_vector_literal(cv_vector)
    score = float(parsed_data.get("matching_score", 0.0))
    seniority = str(parsed_data.get("seniority_estimate", "Unknown"))[:20]
    years_experience = parsed_data.get("years_experience", 0)
    try:
        years_experience = int(years_experience)
    except (TypeError, ValueError):
        years_experience = 0

    conn = psycopg.connect(
        host=host,
        port=secret["port"],
        user=secret["username"],
        password=secret["password"],
        dbname=dbname,
        connect_timeout=RDS_CONNECT_TIMEOUT,
        sslmode=RDS_SSL_MODE,
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidate_embeddings (
                        user_id,
                        embedding,
                        raw_text
                    )
                    VALUES (%s, %s::vector, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        raw_text = EXCLUDED.raw_text,
                        updated_at = NOW();
                    """
                    ,
                    (user_id, vector_literal, masked_cv_text[:120000]),
                )
                cur.execute(
                    """
                    INSERT INTO candidate_profiles_ai (
                        user_id,
                        extracted_profile,
                        seniority_estimate,
                        years_experience,
                        matching_score,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        extracted_profile = EXCLUDED.extracted_profile,
                        seniority_estimate = EXCLUDED.seniority_estimate,
                        years_experience = EXCLUDED.years_experience,
                        matching_score = EXCLUDED.matching_score,
                        updated_at = NOW();
                    """,
                    (user_id, Jsonb(parsed_data), seniority, years_experience, score),
                )
        return True
    finally:
        conn.close()

def save_to_pgvector(
    candidate_id: str,
    cv_vector: list[float],
    parsed_data: dict,
    masked_cv_text: str,
) -> bool:
    """Save embedding to PostgreSQL pgvector. Enabled via feature flag."""
    if not ENABLE_PGVECTOR:
        return False

    if not RDS_SECRET_ARN:
        print("WARNING: RDS_SECRET_ARN missing; pgvector persistence skipped.")
        return False

    if not cv_vector:
        print("WARNING: Empty vector; skipping pgvector persistence.")
        return False

    try:
        if _upsert_pgvector_with_psycopg2(candidate_id, cv_vector, parsed_data, masked_cv_text):
            print(f"INFO: pgvector upsert succeeded via psycopg2 for {candidate_id}.")
            return True
    except ModuleNotFoundError:
        print("WARNING: psycopg2 unavailable; trying psycopg.")
    except Exception as exc:
        print(f"WARNING: pgvector upsert failed via psycopg2: {exc}")

    try:
        if _upsert_pgvector_with_psycopg(candidate_id, cv_vector, parsed_data, masked_cv_text):
            print(f"INFO: pgvector upsert succeeded via psycopg for {candidate_id}.")
            return True
    except ModuleNotFoundError:
        print("WARNING: psycopg unavailable; package psycopg2/psycopg in Lambda artifact.")
        return False
    except Exception as exc:
        print(f"WARNING: pgvector upsert failed via psycopg: {exc}")
        return False

    return False


# ---------------------------------------------------------------------------
# DynamoDB: Result Persistence
# ---------------------------------------------------------------------------

def save_to_dynamodb(
    profile_id: str,
    file_key: str,
    parsed_data: dict,
    success: bool,
    error_message: str = "",
    scoring_details: dict | None = None,
    interview_guide: list | None = None,
    masking_report: dict | None = None,
) -> None:
    """Persist full CV processing result to DynamoDB."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    now_str = now_iso()
    
    def to_decimal(obj):
        return json.loads(json.dumps(obj or {}), parse_float=Decimal)

    status = "SUCCEEDED" if success else "FAILED"
    item = {
        "PK": f"CANDIDATE#{profile_id}",
        "SK": f"CV#{file_key}",
        "GSI1PK": "CANDIDATE",
        "GSI1SK": now_str,
        "candidateId": str(profile_id),
        "objectKey": str(file_key),
        "type": "CV_PARSE_RESULT",
        "parseStatus": status,
        "updatedAt": now_str,
        "expiresAt": expires_at_epoch(),
        "parsedResult": to_decimal(parsed_data),
        "errorMessage": error_message,
        "scoringDetails": to_decimal(scoring_details),
        "interviewGuide": interview_guide or [],
        "maskingReport": masking_report or {},
    }

    table.put_item(Item=item)


# ===========================================================================
# LAMBDA ENTRY POINT
# ===========================================================================

def lambda_handler(event, context):
    """
    Full CV processing pipeline:
      1. Textract → 2. PII Mask → 3. Claude Analysis → 4. Interview Guide
      5. Embed → 6. Cross-Encoder → 7. Hybrid Score → 8. Persist → 9. Index

    Input: payload from IngestionTrigger with bucket, file_key, jd_text, etc.
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

        print(f"INFO: VectorOps execution started. Event keys: {list(event.keys())}")

        preprocessed_payload = event.get("payload")

        if isinstance(preprocessed_payload, dict):
            # Step Functions path: text already processed by text_processor.
            print("INFO: Using preprocessed payload from Step Functions.")
            
            # Extract root-level fields from payload if missing
            if profile_id == "unknown" and preprocessed_payload.get("profile_id"):
                profile_id = preprocessed_payload.get("profile_id")
            if file_key == "unknown" and preprocessed_payload.get("file_key"):
                file_key = preprocessed_payload.get("file_key")
            if not bucket and preprocessed_payload.get("bucket"):
                bucket = preprocessed_payload.get("bucket")
            if job_id == "unknown" and preprocessed_payload.get("job_id"):
                job_id = preprocessed_payload.get("job_id")
                
            masked_cv_text = preprocessed_payload.get("masked_cv_text", "")
            parsed_data = preprocessed_payload.get("parsed_data", {})
            interview_guide = preprocessed_payload.get("interview_guide", [])
            masking_report = preprocessed_payload.get("masking_report", {})

            if preprocessed_payload.get("jd_text"):
                jd_text = preprocessed_payload.get("jd_text")
            if preprocessed_payload.get("job_title"):
                job_title = preprocessed_payload.get("job_title")
            if preprocessed_payload.get("required_skills") is not None:
                required_skills = preprocessed_payload.get("required_skills")
            if preprocessed_payload.get("jd_vector") is not None:
                jd_vector = preprocessed_payload.get("jd_vector")
        
        # Validation AFTER payload propagation
        if not bucket:
            raise ValueError("Missing bucket name")
        if profile_id == "unknown":
            raise ValueError("Cannot derive candidate profile id")

        print(f"INFO: VectorOps routing — profile={profile_id}, job={job_id}, key={file_key}")

        if not isinstance(preprocessed_payload, dict):
            # Backward-compatible path: VectorOps performs full flow.
            print("INFO: No preprocessed payload, running full extraction path.")

            # ── PHASE 1: Text Extraction & Analysis ──────────────────
            raw_cv_text = extract_text_from_s3(bucket, file_key)
            print(f"INFO: Textract complete — {len(raw_cv_text)} chars extracted")

            print("INFO: Running blind screening (PII masking).")
            masked_cv_text, masking_report = mask_pii_entities(raw_cv_text)

            print("INFO: Running Claude Agent 1 (skill extraction).")
            parsed_data = parse_and_evaluate_cv(
                masked_cv_text, jd_text, job_title, required_skills
            )

            print("INFO: Running Claude Agent 2 (interview questions).")
            interview_guide = generate_interview_guide(
                parsed_data, job_title, required_skills
            )

        # ── PHASE 2: Vector Operations & Scoring ─────────────────────

        # [5] Bi-Encoder: embed masked CV
        print("INFO: Running Bi-Encoder (Cohere).")
        cv_vector = get_cohere_embedding(masked_cv_text)

        scoring_details = {"method": "none", "reason": "No JD provided for scoring"}
        
        # Only perform scoring if we have a JD
        if jd_text and jd_text != "Evaluate general technical skills.":
            if jd_vector is not None:
                print("INFO: Using cached JD vector.")
            else:
                print("INFO: Computing JD embedding.")
                jd_vector = get_cohere_embedding(jd_text)

            bi_raw = calculate_cosine_similarity(cv_vector, jd_vector)
            bi_score_pct = round(bi_raw * 100, 2)
            print(f"INFO: Bi-Encoder score: {bi_score_pct}%")

            # [6] Cross-Encoder: reranking
            print("INFO: Running Cross-Encoder.")
            cross_score = compute_cross_encoder_score(masked_cv_text, jd_text)
            if cross_score is not None:
                print(f"INFO: Cross-Encoder score: {round(cross_score * 100, 2)}%")

            # [7] Hybrid blend
            final_score, scoring_details = compute_hybrid_score(bi_score_pct, cross_score)
            print(f"INFO: Final hybrid score: {final_score}%")

            # Update parsed_data with final score
            parsed_data["matching_score"] = final_score
        else:
            print("INFO: Skipping scoring phase (no JD provided).")
            # Default jd_vector to None if not scoring
            jd_vector = None

        # ── PHASE 3: Return Results for Persistence ────────────────
        return {
            "profile_id": profile_id,
            "file_key": file_key,
            "bucket": bucket,
            "job_id": job_id,
            "job_title": job_title,
            "masked_cv_text": masked_cv_text,
            "cv_vector": cv_vector,
            "parsed_data": parsed_data,
            "scoring_details": scoring_details,
            "interview_guide": interview_guide,
            "masking_report": masking_report,
            "jd_text": jd_text,
            "jd_vector": jd_vector
        }

    except Exception as exc:
        print(f"ERROR: VectorOps failed for {profile_id}: {exc}")
        raise
