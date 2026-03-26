# =============================================================================
# candidate_ranking_engine.py — SmartHire AI | Candidate Ranking Engine (Lambda)
#
# Responsibility: Given a newly processed Job Description vector, find the
# top matching candidates in the pgvector pool and build a ranked shortlist
# for the recruiter's dashboard with AI-generated candidate snapshots.
#
# This implements the RECRUITER SIDE of bidirectional matching:
#   Recruiter posts JD → this Lambda finds and ranks matching candidates
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  PIPELINE                                                                │
# │  [1] Receive jd_vector + jd_text + job_id from vector_persister         │
# │  [2] Re-embed jd_text as search_query (asymmetric search)               │
# │  [3] pgvector ANN: query vector <-> candidate_embeddings → top 50       │
# │  [4] Fetch candidate profile (parsed_data) from DynamoDB for each       │
# │  [5] Fetch masked_cv_text from RDS raw_text column for cross-encoding   │
# │  [6] Cross-Encoder: re-rank top 50 → top 15 by semantic precision       │
# │  [7] Claude Agent 3: generate recruiter-facing candidate snapshot        │
# │  [8] DynamoDB: save CANDIDATE_RANKING under JOB#<id>                   │
# │  [9] AppSync: push real-time notification to recruiter dashboard         │
# └─────────────────────────────────────────────────────────────────────────┘
#
# Trigger: Invoked asynchronously by vector_persister (Branch A — JD flow)
# after the JD embedding is stored in RDS.
#
# Input event schema:
# {
#   "job_id":      "uuid",
#   "jd_text":     "...(full JD text)...",
#   "jd_vector":   [0.12, 0.45, ...],    # 1024-dim float list (optional)
#   "job_title":   "Senior Backend Engineer",
#   "required_skills": ["AWS Lambda", ".NET", "DynamoDB"]
# }
#
# Output (DynamoDB write + AppSync push):
# {
#   "PK": "JOB#<job_id>",
#   "SK": "CANDIDATE_RANKING",
#   "rankedCandidates": [
#     {
#       "candidateId": "...",
#       "seniority": "Senior",
#       "yearsExperience": 5,
#       "topSkills": ["AWS", ".NET", "Lambda"],
#       "matchScore": 87.3,
#       "candidateSnapshot": "AI-generated 2-3 sentence recruiter summary",
#       "interviewGuide": [...],   # from earlier pipeline (Agent 2)
#       "applicationStatus": "POOL"  # not yet applied
#     }
#   ]
# }
# =============================================================================

import json
import math
import os
import ssl
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.config import Config

from appsync_notifier import notify_recruiter_candidate_ranking


# ---------------------------------------------------------------------------
# AWS Clients
# ---------------------------------------------------------------------------

retry_config = Config(
    region_name="ap-southeast-1",
    retries={"max_attempts": 10, "mode": "adaptive"},
)

bedrock = boto3.client("bedrock-runtime", config=retry_config)
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
_secretsmanager = boto3.client("secretsmanager", region_name="ap-southeast-1")


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

# Matching parameters
# How many candidates to pull from pgvector before re-ranking
ANN_POOL_SIZE = int(os.environ.get("RANKING_ANN_POOL_SIZE", "50"))
# How many candidates to keep after Cross-Encoder re-ranking
FINAL_RANKING_COUNT = int(os.environ.get("FINAL_RANKING_COUNT", "15"))
# Minimum hybrid score to include in ranking (eliminates clearly poor matches)
MIN_SCORE_THRESHOLD = float(os.environ.get("RANKING_MIN_SCORE", "25.0"))

# Hybrid blending weights
HYBRID_WEIGHT_BI = float(os.environ.get("RANKING_WEIGHT_BI", "0.30"))
HYBRID_WEIGHT_CROSS = float(os.environ.get("RANKING_WEIGHT_CROSS", "0.70"))

# Feature flags
ENABLE_CROSS_ENCODER = os.environ.get("ENABLE_CROSS_ENCODER", "true").lower() == "true"
ENABLE_CANDIDATE_SNAPSHOT = os.environ.get("ENABLE_CANDIDATE_SNAPSHOT", "true").lower() == "true"


# ---------------------------------------------------------------------------
# RDS Connection Helper
# ---------------------------------------------------------------------------

_rds_secret_cache: dict | None = None


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
# Step 2: JD Query Embedding (Asymmetric Search)
# ---------------------------------------------------------------------------

def get_cohere_query_embedding(text: str) -> list[float]:
    """
    Embed JD text as a search_query for asymmetric ANN search.

    When the JD is the *searcher* looking for matching candidates,
    we use input_type="search_query" so it aligns correctly with the
    candidate_embeddings (stored as "search_document").

    If the caller already has a stored jd_vector (from jd_parser), we
    do NOT use it for this search — that stored vector was generated as
    "search_document". We always generate a fresh "search_query" vector.
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
# Step 3: pgvector ANN Search — Find Top N Matching Candidates
# ---------------------------------------------------------------------------

def search_matching_candidates(
    query_vector: list[float],
    top_n: int = ANN_POOL_SIZE,
) -> list[dict]:
    """
    Query RDS pgvector for the top N candidates closest to the JD query vector.

    Joins candidate_embeddings with candidate_profiles_ai to get seniority
    and years_experience for pre-filtering. Also retrieves the masked raw_text
    from candidate_embeddings for use in Cross-Encoder scoring.

    The raw_text stored in RDS is the masked CV text (PII already removed),
    safe to pass to the Cross-Encoder without privacy concerns.

    Returns list of dicts: candidateId, distance, biScore, seniority,
    yearsExperience, rawCvText (masked), matchingScore (from previous analysis)
    """
    if not RDS_SECRET_ARN:
        print("WARNING: RDS not configured. Skipping pgvector candidate search.")
        return []

    vector_literal = _build_vector_literal(query_vector)

    try:
        conn = _get_rds_connection()
        try:
            rows = conn.run(
                """
                SELECT
                    e.user_id                       AS candidate_id,
                    (e.embedding <-> :vec::vector)  AS distance,
                    e.raw_text                      AS masked_cv_text,
                    p.seniority_estimate            AS seniority,
                    p.years_experience              AS years_experience,
                    p.matching_score                AS previous_score,
                    p.extracted_profile             AS profile_json
                FROM candidate_embeddings e
                LEFT JOIN candidate_profiles_ai p ON e.user_id = p.user_id
                ORDER BY distance ASC
                LIMIT :top_n;
                """,
                vec=vector_literal,
                top_n=top_n,
            )

            results = []
            for row in rows:
                (candidate_id, distance, masked_cv_text,
                 seniority, years_exp, prev_score, profile_json) = row

                # L2 distance → percentage score (same formula as job_suggestion_engine)
                bi_score = max(0.0, round(100.0 * (1.0 - float(distance) / 1.4), 2))

                # Parse extracted_profile JSON (stored as JSONB text in RDS)
                parsed_profile = {}
                if profile_json:
                    try:
                        parsed_profile = json.loads(profile_json) if isinstance(profile_json, str) else profile_json
                    except Exception:
                        pass

                results.append({
                    "candidateId": str(candidate_id),
                    "distance": round(float(distance), 6),
                    "biScore": bi_score,
                    "maskedCvText": masked_cv_text or "",
                    "seniority": str(seniority) if seniority else "Unknown",
                    "yearsExperience": int(years_exp) if years_exp else 0,
                    "previousScore": float(prev_score) if prev_score else 0.0,
                    "parsedProfile": parsed_profile,
                })

            print(f"INFO: pgvector ANN returned {len(results)} candidate(s).")
            return results

        finally:
            conn.close()

    except Exception as exc:
        print(f"ERROR: pgvector candidate search failed: {exc}")
        return []


# ---------------------------------------------------------------------------
# Step 4: Fetch Candidate Profile from DynamoDB
# ---------------------------------------------------------------------------

def fetch_candidate_profile_from_dynamodb(candidate_id: str) -> dict:
    """
    Fetch the most recent CV parse result (skills, interview guide) from DynamoDB.

    We query the DynamoDB hot store to get the interviewGuide (from Agent 2)
    and any additional enriched data that the RDS profile_json might not have.

    Uses a Query on PK=CANDIDATE#<id> to get the most recent SK=CV#...
    (sorted by GSI1SK timestamp — latest first).

    Returns empty dict if not found; calling code handles gracefully.
    """
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        # Query most recent CV parse result for this candidate
        response = table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues={
                ":pk": f"CANDIDATE#{candidate_id}",
                ":sk_prefix": "CV#",
            },
            ScanIndexForward=False,  # Descending: most recent first
            Limit=1,
        )
        items = response.get("Items", [])
        if items:
            item = items[0]
            return {
                "interviewGuide": item.get("interviewGuide", []),
                "maskingReport": item.get("maskingReport", {}),
                "scoringDetails": item.get("scoringDetails", {}),
            }
    except Exception as exc:
        print(f"WARNING: DynamoDB profile fetch failed for {candidate_id}: {exc}")

    return {}


# ---------------------------------------------------------------------------
# Step 6: Cross-Encoder Re-Ranking
# ---------------------------------------------------------------------------

_cross_encoder_instance = None


def _get_cross_encoder():
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


def cross_encode_jd_vs_candidates(
    jd_text: str,
    candidates: list[dict],
) -> list[dict]:
    """
    Re-rank candidate matches using Cross-Encoder.

    JD text is the query. Each candidate's masked CV text is chunked into
    overlapping 800-char windows. We score (jd_query, cv_chunk) pairs and
    use weighted top-3 averaging (0.5/0.3/0.2) to reward candidates with
    multiple strong, relevant sections.

    This is the standard direction: JD as query, CV as passage — consistent
    with how ms-marco was trained for passage retrieval.

    Modifies candidates in-place (adds crossScore field).
    Returns candidates sorted by crossScore descending.
    """
    ce = _get_cross_encoder()
    if ce is None:
        for c in candidates:
            c["crossScore"] = None
        return candidates

    jd_query = (jd_text or "")[:1500].strip()
    if not jd_query:
        for c in candidates:
            c["crossScore"] = None
        return candidates

    print(f"INFO: Cross-Encoder scoring {len(candidates)} candidate(s).")

    for candidate in candidates:
        cv_text = (candidate.get("maskedCvText", "") or "").strip()
        if not cv_text:
            candidate["crossScore"] = None
            continue

        # Chunk CV into overlapping windows
        chunks = []
        chunk_size, overlap = 800, 150
        step = chunk_size - overlap
        start = 0
        while start < len(cv_text) and len(chunks) < 12:
            end = start + chunk_size
            chunk = cv_text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            if end >= len(cv_text):
                break
            start += step

        if not chunks:
            candidate["crossScore"] = None
            continue

        pairs = [(jd_query, chunk) for chunk in chunks]
        raw_logits = ce.predict(pairs)
        normalized = [_sigmoid(float(logit)) for logit in raw_logits]

        # Weighted top-3 average: rewards depth, not just one lucky chunk
        top_k = sorted(normalized, reverse=True)[:3]
        raw_weights = [0.5, 0.3, 0.2][: len(top_k)]
        weight_sum = sum(raw_weights)
        weights = [w / weight_sum for w in raw_weights]
        aggregated = sum(s * w for s, w in zip(top_k, weights))

        candidate["crossScore"] = round(aggregated * 100, 2)

    return candidates


# ---------------------------------------------------------------------------
# Hybrid Score
# ---------------------------------------------------------------------------

def compute_hybrid_score(bi_score: float, cross_score: float | None) -> float:
    if cross_score is None:
        return round(bi_score, 2)
    blended = HYBRID_WEIGHT_BI * (bi_score / 100.0) + HYBRID_WEIGHT_CROSS * (cross_score / 100.0)
    return round(blended * 100, 2)


# ---------------------------------------------------------------------------
# Step 7: Claude Agent 3 — Candidate Snapshot for Recruiter
# ---------------------------------------------------------------------------

def generate_candidate_snapshot(
    candidate: dict,
    job_title: str,
    required_skills: list[str],
    jd_preview: str,
) -> str:
    """
    Generate a concise, recruiter-facing 2-3 sentence candidate snapshot.

    This tells the recruiter WHY this candidate is in the shortlist — what
    specific skills match, what experience level they bring, and any notable gap.

    Written in third person ("The candidate has..." / "They bring...").
    Temperature=0.3 for slight variety while keeping factual accuracy high.

    This is Claude Agent 3 for the recruiter side — symmetric counterpart
    to the match explanation generated in job_suggestion_engine for candidates.
    """
    if not ENABLE_CANDIDATE_SNAPSHOT:
        return "Candidate matches key requirements based on vector similarity analysis."

    profile = candidate.get("parsedProfile", {})
    seniority = candidate.get("seniority", "Unknown")
    years_exp = candidate.get("yearsExperience", 0)
    backend = profile.get("backend_skills", [])
    frontend = profile.get("frontend_skills", [])
    devops = profile.get("devops_skills", [])
    all_skills = backend + frontend + devops
    gaps = profile.get("gaps", "")
    strengths = profile.get("strengths", "")

    system_prompt = f"""You are a recruiter assistant AI writing concise candidate summaries for hiring managers.

Write exactly 2-3 sentences summarizing why this candidate is a strong match for the "{job_title}" role.

Rules:
- Write in third person ("The candidate...", "They...")
- Be specific: mention actual skills that match actual requirements
- Include the candidate's seniority and experience level naturally
- Optionally mention 1 gap if it's significant (do not sugarcoat)
- Do NOT mention match scores or numbers
- Do NOT be generic or use filler phrases
- Output ONLY the snapshot text. No JSON, no labels."""

    user_message = f"""Candidate profile:
- Seniority: {seniority}, {years_exp} year(s) experience
- Technical skills: {", ".join(all_skills[:15]) if all_skills else "Not specified"}
- Strengths: {strengths[:250] if strengths else "Not provided"}
- Notable gaps: {gaps[:250] if gaps else "None identified"}

Role being hired for:
- Title: {job_title}
- Required skills: {", ".join(required_skills[:10]) if required_skills else "Not specified"}
- JD excerpt: {jd_preview[:600]}

Write the 2-3 sentence recruiter snapshot."""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 180,
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
        snapshot = response_data.get("content", [{}])[0].get("text", "").strip()
        return snapshot or "Candidate demonstrates relevant technical experience for this role."

    except Exception as exc:
        print(f"WARNING: Candidate snapshot generation failed (non-fatal): {exc}")
        return "Candidate shows strong alignment with required technical skills."


# ---------------------------------------------------------------------------
# Step 8: DynamoDB Persistence
# ---------------------------------------------------------------------------

def save_ranking_to_dynamodb(
    job_id: str,
    ranked_candidates: list[dict],
    updated_at: str,
) -> None:
    """
    Write ranked candidate list to DynamoDB under the job's partition key.

    Single Table Design:
      PK = JOB#<job_id>
      SK = CANDIDATE_RANKING

    Overwrites any previous ranking for the same job, so re-posting the JD
    or re-running the engine always produces a fresh ranked list.

    The interviewGuide from each candidate is included so the recruiter
    sees the AI-generated interview questions directly on the dashboard.
    """
    table = dynamodb.Table(DYNAMODB_TABLE)

    def to_decimal_safe(obj):
        return json.loads(json.dumps(obj), parse_float=Decimal)

    item = {
        "PK": f"JOB#{job_id}",
        "SK": "CANDIDATE_RANKING",
        "GSI1PK": "CANDIDATE_RANKING",
        "GSI1SK": updated_at,
        "type": "CANDIDATE_RANKING",
        "jobId": str(job_id),
        "rankedCandidates": to_decimal_safe(ranked_candidates),
        "candidateCount": len(ranked_candidates),
        "updatedAt": updated_at,
    }

    table.put_item(Item=item)
    print(f"INFO: {len(ranked_candidates)} ranked candidate(s) saved for job {job_id}.")


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Find and rank the best candidates in the pool for a new job posting.

    Expected to be invoked asynchronously (InvocationType=Event) by
    vector_persister (Branch A — JD flow) after the JD embedding is stored.

    Full pipeline:
      [1] Validate input
      [2] Re-embed JD as search_query (asymmetric search)
      [3] pgvector ANN: find top 50 candidates by vector distance
      [4] Fetch enriched DynamoDB profile per candidate (interview guide)
      [5] Cross-Encoder re-rank → top 15 by semantic precision
      [6] Compute hybrid scores; filter by MIN_SCORE_THRESHOLD
      [7] Claude Agent 3: generate candidate snapshot for recruiter per candidate
      [8] Save CANDIDATE_RANKING to DynamoDB
      [9] AppSync: push real-time ranked list to recruiter dashboard
    """
    job_id = event.get("job_id", "unknown")

    try:
        jd_text = event.get("jd_text", "")
        job_title = event.get("job_title", "Unknown Role")
        required_skills = event.get("required_skills", [])

        if job_id == "unknown" or not jd_text:
            raise ValueError("job_id and jd_text are required.")

        print(f"INFO: CandidateRankingEngine started for job={job_id} ({job_title})")

        # ── [2] Re-embed JD as search_query ──────────────────────────────
        print("INFO: Generating search_query embedding for JD.")
        query_vector = get_cohere_query_embedding(jd_text)

        # ── [3] ANN vector search against candidate_embeddings ──────────
        print(f"INFO: Searching top {ANN_POOL_SIZE} candidates via pgvector.")
        ann_matches = search_matching_candidates(query_vector, top_n=ANN_POOL_SIZE)

        if not ann_matches:
            print("WARNING: No candidate matches returned from pgvector.")
            updated_at = datetime.now(timezone.utc).isoformat()
            save_ranking_to_dynamodb(job_id, [], updated_at)
            return {"statusCode": 200, "body": "No candidates found. Ranking saved as empty."}

        # ── [4] Fetch enriched DynamoDB profiles ─────────────────────────
        print(f"INFO: Fetching DynamoDB profiles for {len(ann_matches)} candidate(s).")
        for candidate in ann_matches:
            ddb_profile = fetch_candidate_profile_from_dynamodb(candidate["candidateId"])
            candidate.update(ddb_profile)

        # ── [5] Cross-Encoder re-rank ─────────────────────────────────────
        print("INFO: Running Cross-Encoder re-ranking.")
        ann_matches = cross_encode_jd_vs_candidates(jd_text, ann_matches)

        # ── [6] Hybrid score + filter + sort ──────────────────────────────
        for c in ann_matches:
            c["finalScore"] = compute_hybrid_score(c["biScore"], c.get("crossScore"))

        filtered = [c for c in ann_matches if c["finalScore"] >= MIN_SCORE_THRESHOLD]
        filtered.sort(key=lambda c: c["finalScore"], reverse=True)
        top_candidates = filtered[:FINAL_RANKING_COUNT]

        print(f"INFO: {len(top_candidates)} candidate(s) passed threshold ({MIN_SCORE_THRESHOLD}%).")

        jd_preview = jd_text[:600]

        # ── [7] Claude Agent 3: candidate snapshots ───────────────────────
        ranked_output = []
        for rank, candidate in enumerate(top_candidates, start=1):
            cid = candidate["candidateId"]
            print(f"INFO: Generating snapshot for candidate {rank}/{len(top_candidates)} ({cid}).")

            snapshot = generate_candidate_snapshot(
                candidate, job_title, required_skills, jd_preview
            )

            profile = candidate.get("parsedProfile", {})
            all_skills = (
                profile.get("backend_skills", [])
                + profile.get("frontend_skills", [])
                + profile.get("devops_skills", [])
            )

            ranked_output.append({
                "rank": rank,
                "candidateId": cid,
                "seniority": candidate.get("seniority", "Unknown"),
                "yearsExperience": candidate.get("yearsExperience", 0),
                "topSkills": all_skills[:10],
                "biEncoderScore": candidate["biScore"],
                "crossEncoderScore": candidate.get("crossScore"),
                "finalScore": candidate["finalScore"],
                "candidateSnapshot": snapshot,
                "interviewGuide": candidate.get("interviewGuide", []),
                "applicationStatus": "POOL",  # Has not applied — discovered by AI
            })

        # ── [8] Save to DynamoDB ───────────────────────────────────────────
        updated_at = datetime.now(timezone.utc).isoformat()
        save_ranking_to_dynamodb(job_id, ranked_output, updated_at)

        # ── [9] AppSync real-time push ────────────────────────────────────
        notify_recruiter_candidate_ranking(job_id, ranked_output, updated_at)

        print(f"INFO: CandidateRankingEngine complete. {len(ranked_output)} candidate(s) ranked for job {job_id}.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "SUCCESS",
                "job_id": job_id,
                "ranked_count": len(ranked_output),
            }),
        }

    except Exception as exc:
        print(f"ERROR: CandidateRankingEngine failed for job={job_id}: {exc}")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "FAILED", "error": str(exc)}),
        }
