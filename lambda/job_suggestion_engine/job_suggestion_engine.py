# =============================================================================
# job_suggestion_engine.py — SmartHire AI | Job Suggestion Engine (Lambda)
#
# Responsibility: Given a candidate's stored CV vector and profile, find the
# top matching Job Descriptions in the pgvector pool and return personalized
# job suggestions to the candidate with AI-generated match explanations.
#
# This implements the CANDIDATE SIDE of bidirectional matching:
#   Candidate uploads CV globally → this Lambda finds matching JDs
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  PIPELINE                                                                │
# │  [1] Receive cv_vector + parsed_data from vector_persister              │
# │  [2] Re-embed masked_cv_text as search_query (asymmetric search)        │
# │  [3] pgvector ANN: query vector <-> job_embeddings → top 20 jobs        │
# │  [4] Fetch JD text + metadata from DynamoDB for each candidate job      │
# │  [5] Cross-Encoder: re-rank top 20 → top 5 by semantic precision        │
# │  [6] Claude Agent 3: generate personalized match explanation per job    │
# │  [7] DynamoDB: save JOB_SUGGESTIONS under CANDIDATE#<id>               │
# │  [8] AppSync: push real-time notification to candidate frontend         │
# └─────────────────────────────────────────────────────────────────────────┘
#
# Trigger: Invoked asynchronously by vector_persister after CV is stored.
#
# Input event schema:
# {
#   "profile_id":     "uuid",
#   "file_key":       "candidates/uuid/cv.pdf",
#   "masked_cv_text": "...(up to 50k chars)...",
#   "cv_vector":      [0.12, 0.45, ...],    # 1024-dim float list
#   "parsed_data":    { "seniority_estimate": "...", "backend_skills": [...], ... }
# }
#
# Output (DynamoDB write + AppSync push):
# {
#   "PK": "CANDIDATE#<profile_id>",
#   "SK": "JOB_SUGGESTIONS",
#   "suggestions": [
#     {
#       "jobId": "...",
#       "jobTitle": "...",
#       "companyName": "...",
#       "seniority": "...",
#       "requiredSkills": [...],
#       "biEncoderScore": 78.5,
#       "crossEncoderScore": 82.1,
#       "finalScore": 80.8,
#       "matchExplanation": "AI-generated paragraph why this job suits candidate"
#     }
#   ]
# }
#
# Deployment: Lambda Container Image.
#   Requires sentence-transformers for Cross-Encoder.
#   Shares the same container image as vector_ops Lambda.
# =============================================================================

import json
import math
import os
import ssl
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.config import Config

# Import shared AppSync utility (must be in the same Lambda deployment package)
from appsync_notifier import notify_candidate_job_suggestions


# ---------------------------------------------------------------------------
# AWS Clients
# ---------------------------------------------------------------------------

retry_config = Config(
    region_name="ap-southeast-1",
    retries={"max_attempts": 10, "mode": "adaptive"},
)

bedrock = boto3.client("bedrock-runtime", config=retry_config)
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAUDE_MODEL_ID = os.environ.get("CLAUDE_MODEL_ID") or os.environ.get(
    "BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0"
)
COHERE_MODEL_ID = os.environ.get("COHERE_MODEL_ID", "cohere.embed-english-v3")

DYNAMODB_TABLE = (
    os.environ.get("DYNAMODB_TABLE_NAME")
    or os.environ.get("DYNAMODB_TABLE")
    or "SmartHire_ApplicationTracking"
)
DYNAMODB_JOBS_TABLE = os.environ.get("DYNAMODB_JOBS_TABLE", "SmartHire_Jobs")

# pgvector / RDS
RDS_PROXY_ENDPOINT = os.environ.get("RDS_PROXY_ENDPOINT", "")
RDS_SECRET_ARN = os.environ.get("RDS_SECRET_ARN", "")
RDS_DATABASE = os.environ.get("RDS_DATABASE", "smarthiredb")
RDS_SSL_MODE = os.environ.get("RDS_SSL_MODE", "require")
RDS_CONNECT_TIMEOUT = int(os.environ.get("RDS_CONNECT_TIMEOUT", "8"))

# Cross-Encoder
CROSS_ENCODER_MODEL_ID = os.environ.get(
    "CROSS_ENCODER_MODEL_ID", "cross-encoder/ms-marco-MiniLM-L-6-v2"
)
CROSS_ENCODER_BAKED_PATH = os.environ.get(
    "CROSS_ENCODER_MODEL_PATH", "/opt/ml/cross_encoder"
)

# Matching thresholds
# Step 1 ANN: how many jobs to pull from pgvector before re-ranking
ANN_CANDIDATE_POOL_SIZE = int(os.environ.get("ANN_CANDIDATE_POOL_SIZE", "20"))
# Step 5 Cross-Encoder: how many top jobs to keep after re-ranking
FINAL_SUGGESTION_COUNT = int(os.environ.get("FINAL_SUGGESTION_COUNT", "5"))
# Minimum final score to include in suggestions (0-100)
MIN_SCORE_THRESHOLD = float(os.environ.get("MIN_SCORE_THRESHOLD", "30.0"))

# Hybrid score weights for suggestion scoring
# Using equal weights here because for global matching (no specific JD),
# bi-encoder provides good broad matching; cross-encoder adds precision.
HYBRID_WEIGHT_BI = float(os.environ.get("SUGGESTION_WEIGHT_BI", "0.35"))
HYBRID_WEIGHT_CROSS = float(os.environ.get("SUGGESTION_WEIGHT_CROSS", "0.65"))

# Feature flags
ENABLE_CROSS_ENCODER = os.environ.get("ENABLE_CROSS_ENCODER", "true").lower() == "true"
ENABLE_MATCH_EXPLANATION = os.environ.get("ENABLE_MATCH_EXPLANATION", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Secrets / RDS Credentials (cached singleton)
# ---------------------------------------------------------------------------

_rds_secret_cache: dict | None = None
_secretsmanager = boto3.client("secretsmanager", region_name="ap-southeast-1")


def _resolve_rds_credentials() -> dict:
    global _rds_secret_cache
    if _rds_secret_cache:
        return _rds_secret_cache

    if not RDS_SECRET_ARN:
        raise RuntimeError("RDS_SECRET_ARN not configured.")

    resp = _secretsmanager.get_secret_value(SecretId=RDS_SECRET_ARN)
    parsed = json.loads(resp["SecretString"])
    _rds_secret_cache = {
        "username": parsed["username"],
        "password": parsed["password"],
        "host": parsed.get("host", ""),
        "port": int(parsed.get("port", 5432)),
        "dbname": parsed.get("dbname", RDS_DATABASE),
    }
    return _rds_secret_cache


def _get_rds_connection():
    """Open a pg8000 connection to RDS. Caller is responsible for closing."""
    import pg8000.native  # type: ignore

    secret = _resolve_rds_credentials()
    host = RDS_PROXY_ENDPOINT or secret["host"]

    ssl_ctx = None
    if RDS_SSL_MODE in ("require", "verify-full", "verify-ca"):
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    return pg8000.native.Connection(
        user=secret["username"],
        password=secret["password"],
        host=host,
        port=secret["port"],
        database=secret["dbname"],
        ssl_context=ssl_ctx,
        timeout=RDS_CONNECT_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# Step 2: Cohere Query Embedding (Asymmetric Search)
# ---------------------------------------------------------------------------

def get_cohere_query_embedding(text: str) -> list[float]:
    """
    Embed text using Cohere with input_type="search_query".

    IMPORTANT: This is intentionally different from the input_type="search_document"
    used when the CV vector was originally stored in pgvector.

    Cohere embed-v3 is trained for asymmetric search:
      - Documents (CVs, JDs) → embedded with "search_document"
      - Search queries → embedded with "search_query"

    When the candidate's CV is the *searcher* looking for matching jobs,
    the CV text should be re-embedded as a "search_query" so it aligns
    correctly with the job_embeddings (stored as "search_document").
    Using a fresh query embedding here gives materially better recall
    than reusing the stored document vector for ANN search.
    """
    normalized = (text or "").strip()[:2048] or "No content"
    payload = {
        "texts": [normalized],
        "input_type": "search_query",
        "truncate": "END",
    }
    response = bedrock.invoke_model(
        modelId=COHERE_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    return json.loads(response.get("body").read())["embeddings"][0]


def _build_vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{float(v):.8f}" for v in vec) + "]"


# ---------------------------------------------------------------------------
# Step 3: pgvector ANN Search — Find Top N Matching Jobs
# ---------------------------------------------------------------------------

def search_matching_jobs(query_vector: list[float], top_n: int = ANN_CANDIDATE_POOL_SIZE) -> list[dict]:
    """
    Query RDS pgvector for the top N jobs closest to the candidate's query vector.

    Uses L2 distance (<->) operator. For normalized vectors, L2 distance and
    cosine distance produce the same ranking — L2 is faster because pgvector
    can use IVFFlat index with L2 distance natively.

    Joins job_embeddings with Jobs table to get title and active status,
    filtering only is_active=true so candidates only see open positions.

    Returns list of dicts with: jobId, jobTitle, companyName, distance, biScore
    """
    if not RDS_SECRET_ARN:
        print("WARNING: RDS not configured. Skipping pgvector ANN search.")
        return []

    vector_literal = _build_vector_literal(query_vector)

    try:
        conn = _get_rds_connection()
        try:
            rows = conn.run(
                """
                SELECT
                    j."Id"          AS job_id,
                    j."Title"       AS job_title,
                    c."Name"        AS company_name,
                    (e.embedding <-> :vec::vector) AS distance
                FROM job_embeddings e
                JOIN "Jobs" j ON e.job_id = j."Id"
                LEFT JOIN "Companies" c ON j."CompanyId" = c."Id"
                WHERE j."IsActive" = true
                ORDER BY distance ASC
                LIMIT :top_n;
                """,
                vec=vector_literal,
                top_n=top_n,
            )

            results = []
            for row in rows:
                job_id, job_title, company_name, distance = row
                # Convert L2 distance to a rough percentage score.
                # For unit-normalized 1024-dim vectors, L2 distance range is [0, ~1.4].
                # We map this linearly: distance 0 → 100%, distance ≥ 1.4 → 0%.
                bi_score = max(0.0, round(100.0 * (1.0 - float(distance) / 1.4), 2))
                results.append({
                    "jobId": str(job_id),
                    "jobTitle": str(job_title),
                    "companyName": str(company_name) if company_name else "Unknown Company",
                    "distance": round(float(distance), 6),
                    "biScore": bi_score,
                })

            print(f"INFO: pgvector ANN returned {len(results)} job match(es).")
            return results

        finally:
            conn.close()

    except Exception as exc:
        print(f"ERROR: pgvector job search failed: {exc}")
        return []


# ---------------------------------------------------------------------------
# Step 4: Fetch JD Metadata from DynamoDB
# ---------------------------------------------------------------------------

def fetch_jd_details(job_id: str) -> dict:
    """
    Fetch the JD text, required skills, and seniority from DynamoDB.

    The jd_parser stores these at PK=JOB#<id>, SK=METADATA.
    We need the jdText for Cross-Encoder scoring and required_skills
    for the match explanation Claude prompt.

    Returns empty dict if not found (job will be excluded from suggestions).
    """
    try:
        table = dynamodb.Table(DYNAMODB_JOBS_TABLE)
        response = table.get_item(Key={"jobId": str(job_id)})
        if "Item" in response:
            item = response["Item"]
            return {
                "jdText": item.get("jdText", ""),
                "requiredSkills": item.get("requiredSkills", []),
                "seniority": item.get("seniority", ""),
                "parseStatus": item.get("parseStatus", ""),
            }
    except Exception as exc:
        print(f"WARNING: Failed to fetch JD details for {job_id}: {exc}")

    return {}


# ---------------------------------------------------------------------------
# Step 5: Cross-Encoder Re-Ranking
# ---------------------------------------------------------------------------

_cross_encoder_instance = None


def _get_cross_encoder():
    """Load CrossEncoder model singleton. Returns None on failure (non-fatal)."""
    global _cross_encoder_instance

    if _cross_encoder_instance is not None:
        return _cross_encoder_instance

    if not ENABLE_CROSS_ENCODER:
        return None

    try:
        from sentence_transformers import CrossEncoder  # type: ignore

        tmp_cache = "/tmp/cross_encoder_model"

        if os.path.isdir(CROSS_ENCODER_BAKED_PATH):
            path = CROSS_ENCODER_BAKED_PATH
        elif os.path.isdir(tmp_cache):
            path = tmp_cache
        else:
            print("INFO: Downloading cross-encoder (one-time cold-start cost).")
            os.environ["HF_HOME"] = "/tmp/hf_home"
            m = CrossEncoder(CROSS_ENCODER_MODEL_ID, max_length=512)
            m.save(tmp_cache)
            del m
            path = tmp_cache

        _cross_encoder_instance = CrossEncoder(path, max_length=512)
        print("INFO: Cross-encoder ready.")
        return _cross_encoder_instance

    except Exception as exc:
        print(f"WARNING: Cross-encoder unavailable: {exc}")
        return None


def _sigmoid(x: float) -> float:
    x = max(-500.0, min(500.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def cross_encode_cv_vs_jobs(
    masked_cv_text: str,
    candidates: list[dict],
) -> list[dict]:
    """
    Re-rank candidate job matches using Cross-Encoder.

    For the suggestion engine (CV searching for JDs), we flip the pair order
    compared to cv_parser: the JD text is the "query" and the CV is the
    "passage". This is consistent with how ms-marco was trained: the query
    is the information need, and the passage is the document being evaluated.

    Input pairs: (jd_text[:1500], cv_chunk)
    The CV is chunked into 800-char overlapping windows. The maximum
    chunk score (rather than the weighted top-K from cv_parser) is used
    because here we're asking "does any part of this CV satisfy the JD?"

    Returns the input list sorted by cross_score descending, with
    cross_score added to each item.
    """
    ce = _get_cross_encoder()
    if ce is None:
        # Fall back to bi-encoder ranking only
        for item in candidates:
            item["crossScore"] = None
        return candidates

    # Chunk the CV text once and reuse for all jobs
    cv_text = (masked_cv_text or "").strip()
    chunk_size = 800
    overlap = 150
    step = chunk_size - overlap
    cv_chunks = []
    start = 0
    while start < len(cv_text) and len(cv_chunks) < 8:
        end = start + chunk_size
        chunk = cv_text[start:end].strip()
        if chunk:
            cv_chunks.append(chunk)
        if end >= len(cv_text):
            break
        start += step

    if not cv_chunks:
        for item in candidates:
            item["crossScore"] = None
        return candidates

    print(f"INFO: Cross-Encoder scoring {len(candidates)} job(s) against {len(cv_chunks)} CV chunk(s).")

    for item in candidates:
        jd_text = (item.get("jdText", "") or "")[:1500].strip()
        if not jd_text:
            item["crossScore"] = None
            continue

        # Build (jd_query, cv_passage) pairs
        pairs = [(jd_text, chunk) for chunk in cv_chunks]
        raw_logits = ce.predict(pairs)
        normalized = [_sigmoid(float(logit)) for logit in raw_logits]

        # Max pooling: does the best CV chunk satisfy the JD?
        item["crossScore"] = round(max(normalized) * 100, 2) if normalized else None

    return candidates


# ---------------------------------------------------------------------------
# Hybrid Score Computation
# ---------------------------------------------------------------------------

def compute_hybrid_score(bi_score: float, cross_score: float | None) -> float:
    """
    Blend bi-encoder and cross-encoder scores.

    If cross-encoder is unavailable, falls back to bi-encoder only.
    Returned value is in [0, 100] percentage scale.
    """
    if cross_score is None:
        return round(bi_score, 2)

    blended = HYBRID_WEIGHT_BI * (bi_score / 100.0) + HYBRID_WEIGHT_CROSS * (cross_score / 100.0)
    return round(blended * 100, 2)


# ---------------------------------------------------------------------------
# Step 6: Claude Agent 3 — Match Explanation Generator
# ---------------------------------------------------------------------------

def generate_match_explanation(
    parsed_data: dict,
    job: dict,
) -> str:
    """
    Generate a personalized, 2-3 sentence explanation of why this job matches
    the candidate. Written from the candidate's perspective.

    This is Claude Agent 3 — the third AI call in the full pipeline
    (after Agent 1: skill extraction and Agent 2: interview questions).

    Design principle: The explanation must be SPECIFIC, not generic.
    Bad: "This job matches your skills in software development."
    Good: "Your 4 years of .NET backend experience and AWS Lambda work directly
           align with the core requirements. The team's focus on serverless
           microservices maps closely to your recent projects."

    Temperature=0.4 introduces slight variation so identical candidates
    don't receive identical explanations for the same job.
    """
    if not ENABLE_MATCH_EXPLANATION:
        return "This role closely matches your experience and technical background."

    seniority = parsed_data.get("seniority_estimate", "Mid")
    years_exp = parsed_data.get("years_experience", 0)
    backend = parsed_data.get("backend_skills", [])
    frontend = parsed_data.get("frontend_skills", [])
    devops = parsed_data.get("devops_skills", [])
    strengths = parsed_data.get("strengths", "")
    all_skills = backend + frontend + devops

    job_title = job.get("jobTitle", "Unknown Role")
    company = job.get("companyName", "the company")
    required = job.get("requiredSkills", [])
    jd_preview = (job.get("jdText", "") or "")[:800]

    system_prompt = """You are a helpful career advisor AI writing personalized job match explanations for candidates.

Write exactly 2-3 sentences explaining specifically WHY this job is a strong match for this candidate.

Rules:
- Be specific: mention actual skills and how they map to actual job requirements
- Write in second person ("Your experience in X...")
- Do NOT be generic or use vague phrases like "great opportunity"
- Do NOT mention the match score number
- Focus on the strongest 2-3 alignment points
- Output ONLY the explanation text. No JSON, no labels, no markdown."""

    user_message = f"""Candidate profile:
- Seniority: {seniority}, {years_exp} year(s) experience
- Technical skills: {", ".join(all_skills[:15]) if all_skills else "Not specified"}
- Their strengths: {strengths[:300] if strengths else "Not provided"}

Job being matched:
- Title: {job_title} at {company}
- Required skills: {", ".join(required[:10]) if required else "Not specified"}
- JD excerpt: {jd_preview}

Write the 2-3 sentence match explanation for this candidate."""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 200,
        "temperature": 0.4,
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
        explanation = response_data.get("content", [{}])[0].get("text", "").strip()
        return explanation or "This role aligns with your technical background and experience level."

    except Exception as exc:
        print(f"WARNING: Match explanation generation failed (non-fatal): {exc}")
        return "This role closely aligns with your skills and experience."


# ---------------------------------------------------------------------------
# Step 7: DynamoDB Persistence
# ---------------------------------------------------------------------------

def save_suggestions_to_dynamodb(
    profile_id: str,
    suggestions: list[dict],
    updated_at: str,
) -> None:
    """
    Write job suggestions to DynamoDB under the candidate's partition key.

    Single Table Design:
      PK = CANDIDATE#<profile_id>
      SK = JOB_SUGGESTIONS

    This overwrites any previous suggestions for the same candidate,
    so each new CV upload produces a fresh ranked suggestion list.

    The suggestions list is serialized with Decimal conversion because
    DynamoDB does not accept Python float.
    """
    table = dynamodb.Table(DYNAMODB_TABLE)

    def to_decimal_safe(obj):
        return json.loads(json.dumps(obj), parse_float=Decimal)

    item = {
        "PK": f"CANDIDATE#{profile_id}",
        "SK": "JOB_SUGGESTIONS",
        "GSI1PK": "JOB_SUGGESTIONS",
        "GSI1SK": updated_at,
        "type": "JOB_SUGGESTIONS",
        "candidateId": str(profile_id),
        "suggestions": to_decimal_safe(suggestions),
        "suggestionCount": len(suggestions),
        "updatedAt": updated_at,
    }

    table.put_item(Item=item)
    print(f"INFO: {len(suggestions)} suggestion(s) saved for candidate {profile_id}.")


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Find and save top matching jobs for a candidate who uploaded their CV globally.

    Expected to be invoked asynchronously (InvocationType=Event) by
    vector_persister after it stores the candidate's CV embedding in RDS.

    Full pipeline:
      [1] Validate input
      [2] Re-embed masked CV as search_query (asymmetric search)
      [3] pgvector ANN: find top 20 jobs by vector distance
      [4] Fetch JD text/metadata from DynamoDB for each candidate job
      [5] Cross-Encoder re-rank → top 5 by semantic precision
      [6] Compute hybrid scores; filter by MIN_SCORE_THRESHOLD
      [7] Claude Agent 3: generate match explanation per top job
      [8] Save suggestions to DynamoDB
      [9] AppSync: push real-time update to candidate frontend
    """
    profile_id = event.get("profile_id", "unknown")

    try:
        masked_cv_text = event.get("masked_cv_text", "")
        parsed_data = event.get("parsed_data", {})

        if profile_id == "unknown" or not masked_cv_text:
            raise ValueError("profile_id and masked_cv_text are required.")

        print(f"INFO: JobSuggestionEngine started for profile={profile_id}")

        # ── [2] Re-embed CV as search_query ─────────────────────────────
        print("INFO: Generating search_query embedding for CV.")
        query_vector = get_cohere_query_embedding(masked_cv_text)

        # ── [3] ANN vector search against job_embeddings ─────────────────
        print(f"INFO: Searching top {ANN_CANDIDATE_POOL_SIZE} jobs via pgvector.")
        ann_matches = search_matching_jobs(query_vector, top_n=ANN_CANDIDATE_POOL_SIZE)

        if not ann_matches:
            print("WARNING: No job matches returned from pgvector. Saving empty suggestions.")
            updated_at = datetime.now(timezone.utc).isoformat()
            save_suggestions_to_dynamodb(profile_id, [], updated_at)
            return {"statusCode": 200, "body": "No matches found. Suggestions saved as empty."}

        # ── [4] Fetch JD details from DynamoDB ───────────────────────────
        print(f"INFO: Fetching JD details for {len(ann_matches)} candidate job(s).")
        for match in ann_matches:
            jd_details = fetch_jd_details(match["jobId"])
            match.update(jd_details)
            # Skip jobs that failed to parse or have empty JD text
            if not jd_details.get("jdText"):
                match["jdText"] = ""

        # Filter out jobs without JD text (can't cross-encode without it)
        scorable_matches = [m for m in ann_matches if m.get("jdText")]
        print(f"INFO: {len(scorable_matches)} job(s) have JD text for cross-encoding.")

        # ── [5] Cross-Encoder re-rank ──────────────────────────────────
        if scorable_matches:
            print("INFO: Running Cross-Encoder re-ranking.")
            scorable_matches = cross_encode_cv_vs_jobs(masked_cv_text, scorable_matches)

        # ── [6] Compute hybrid score and filter ───────────────────────────
        for match in scorable_matches:
            match["finalScore"] = compute_hybrid_score(
                match["biScore"], match.get("crossScore")
            )

        # Sort by finalScore descending, take top N above threshold
        filtered = [
            m for m in scorable_matches
            if m["finalScore"] >= MIN_SCORE_THRESHOLD
        ]
        filtered.sort(key=lambda m: m["finalScore"], reverse=True)
        top_jobs = filtered[:FINAL_SUGGESTION_COUNT]

        print(f"INFO: {len(top_jobs)} job(s) passed threshold ({MIN_SCORE_THRESHOLD}%).")

        # ── [7] Claude Agent 3: match explanations ────────────────────────
        suggestions = []
        for job in top_jobs:
            print(f"INFO: Generating match explanation for jobId={job['jobId']}.")
            explanation = generate_match_explanation(parsed_data, job)

            suggestions.append({
                "jobId": job["jobId"],
                "jobTitle": job["jobTitle"],
                "companyName": job.get("companyName", "Unknown Company"),
                "seniority": job.get("seniority", ""),
                "requiredSkills": job.get("requiredSkills", []),
                "biEncoderScore": job["biScore"],
                "crossEncoderScore": job.get("crossScore"),
                "finalScore": job["finalScore"],
                "matchExplanation": explanation,
            })

        # ── [8] Save to DynamoDB ──────────────────────────────────────────
        updated_at = datetime.now(timezone.utc).isoformat()
        save_suggestions_to_dynamodb(profile_id, suggestions, updated_at)

        # ── [9] AppSync real-time push ────────────────────────────────────
        notify_candidate_job_suggestions(profile_id, suggestions, updated_at)

        print(f"INFO: JobSuggestionEngine complete. {len(suggestions)} suggestion(s) delivered.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "SUCCESS",
                "profile_id": profile_id,
                "suggestion_count": len(suggestions),
            }),
        }

    except Exception as exc:
        print(f"ERROR: JobSuggestionEngine failed for profile={profile_id}: {exc}")
        # Non-fatal: do not re-raise. A suggestion failure should never block
        # the candidate's CV being saved. Return 200 so the async invoke
        # does not get retried unnecessarily.
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "FAILED", "error": str(exc)}),
        }
