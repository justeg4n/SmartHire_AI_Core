# =============================================================================
# vector_persister.py — SmartHire AI | Persistence Lambda (VPC-bound)
#
# Responsibility: Persist profile, embedding, and match results to:
#   1. Amazon RDS PostgreSQL (pgvector)
#   2. Amazon DynamoDB (Parse Results Table)
#
# Networking: Runs in Private Subnet. Accesses RDS directly. 
#   Accesses AWS APIs (SecretsManager, DynamoDB, S3) via VPC Endpoints.
# =============================================================================

import json
import os
import uuid
import ssl
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import boto3

# ---------------------------------------------------------------------------
# AWS Clients
# ---------------------------------------------------------------------------
dynamodb = boto3.resource("dynamodb")
secretsmanager = boto3.client("secretsmanager")
lambda_client = boto3.client("lambda", region_name="ap-southeast-1")
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DYNAMODB_TABLE = (
    os.environ.get("DYNAMODB_TABLE_NAME")
    or os.environ.get("DYNAMODB_TABLE")
    or "SmartHire_Profiles"
)
RDS_PROXY_ENDPOINT = os.environ.get("RDS_PROXY_ENDPOINT", "")
RDS_DATABASE = os.environ.get("RDS_DATABASE", "smarthiredb")
RDS_SECRET_ARN = os.environ.get("RDS_SECRET_ARN", "")
RDS_SSL_MODE = os.environ.get("RDS_SSL_MODE", "require")
RDS_CONNECT_TIMEOUT = int(os.environ.get("RDS_CONNECT_TIMEOUT", "8"))
ENABLE_PGVECTOR = os.environ.get("ENABLE_PGVECTOR", "true").lower() == "true"
JOB_SUGGESTION_FUNCTION = os.environ.get("JOB_SUGGESTION_FUNCTION_NAME", "")
ENABLE_JOB_SUGGESTIONS = os.environ.get("ENABLE_JOB_SUGGESTIONS", "true").lower() == "true"
CANDIDATE_RANKING_FUNCTION = os.environ.get("CANDIDATE_RANKING_FUNCTION_NAME", "")
ENABLE_CANDIDATE_RANKING = os.environ.get("ENABLE_CANDIDATE_RANKING", "true").lower() == "true"

_rds_secret_cache: dict[str, Any] | None = None

# Utility Helpers
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def expires_at_epoch() -> int:
    PARSE_STATUS_TTL_DAYS = int(os.environ.get("PARSE_STATUS_TTL_DAYS", "30"))
    return int(
        (datetime.now(timezone.utc) + timedelta(days=PARSE_STATUS_TTL_DAYS)).timestamp()
    )

# ---------------------------------------------------------------------------
# RDS pgvector Logic (using pg8000 - Pure Python)
# ---------------------------------------------------------------------------
def _resolve_rds_credentials() -> dict[str, Any]:
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
    return "[" + ",".join(f"{float(v):.8f}" for v in values) + "]"

def _to_user_id(candidate_id: str) -> str | None:
    try:
        return str(uuid.UUID(str(candidate_id)))
    except (TypeError, ValueError):
        return None

def _upsert_pgvector_with_pg8000(
    candidate_id: str,
    cv_vector: list[float],
    parsed_data: dict,
    masked_cv_text: str,
) -> bool:
    import pg8000.native
    
    user_id = _to_user_id(candidate_id)
    if user_id is None:
        raise ValueError(f"candidate_id '{candidate_id}' is not a valid UUID user_id")
    
    secret = _resolve_rds_credentials()
    host = RDS_PROXY_ENDPOINT or secret["host"]
    dbname = secret["dbname"]
    vector_literal = _build_vector_literal(cv_vector)
    score = float(parsed_data.get("matching_score", 0.0))
    seniority = str(parsed_data.get("seniority_estimate", "Unknown"))[:20]
    years_experience = parsed_data.get("years_experience", 0)
    try:
        years_experience = int(years_experience)
    except (TypeError, ValueError):
        years_experience = 0

    ssl_context = None
    if RDS_SSL_MODE in ["require", "verify-full", "verify-ca"]:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    conn = pg8000.native.Connection(
        user=secret["username"],
        password=secret["password"],
        host=host,
        port=secret["port"],
        database=dbname,
        ssl_context=ssl_context,
        timeout=RDS_CONNECT_TIMEOUT
    )
    
    try:
        # Upsert Embeddings
        conn.run(
            "INSERT INTO candidate_embeddings (user_id, embedding, raw_text) "
            "VALUES (:uid, :vec::vector, :txt) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "embedding = EXCLUDED.embedding, raw_text = EXCLUDED.raw_text, updated_at = NOW();",
            uid=user_id, vec=vector_literal, txt=masked_cv_text[:120000]
        )
        
        # Upsert Profiles
        conn.run(
            "INSERT INTO candidate_profiles_ai (user_id, extracted_profile, seniority_estimate, years_experience, matching_score, updated_at) "
            "VALUES (:uid, :profile, :seniority, :exp, :score, NOW()) "
            "ON CONFLICT (user_id) DO UPDATE SET "
            "extracted_profile = EXCLUDED.extracted_profile, "
            "seniority_estimate = EXCLUDED.seniority_estimate, "
            "years_experience = EXCLUDED.years_experience, "
            "matching_score = EXCLUDED.matching_score, updated_at = NOW();",
            uid=user_id, profile=json.dumps(parsed_data), 
            seniority=seniority, exp=years_experience, score=score
        )
        return True
    finally:
        conn.close()

def save_job_to_pgvector(job_id: str, jd_vector: list[float], raw_text: str) -> bool:
    """Persist Job Description embedding to RDS."""
    if not ENABLE_PGVECTOR or not RDS_SECRET_ARN or not jd_vector:
        return False
        
    import pg8000.native
    try:
        secret = _resolve_rds_credentials()
        host = RDS_PROXY_ENDPOINT or secret["host"]
        dbname = secret["dbname"]
        vector_literal = _build_vector_literal(jd_vector)

        ssl_context = None
        if RDS_SSL_MODE in ["require", "verify-full", "verify-ca"]:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        conn = pg8000.native.Connection(
            user=secret["username"],
            password=secret["password"],
            host=host,
            port=secret["port"],
            database=dbname,
            ssl_context=ssl_context,
            timeout=RDS_CONNECT_TIMEOUT
        )
        
        try:
            # Upsert Job Embedding
            # Note: Jobs table uses UUIDs as confirmed by schema
            conn.run(
                "INSERT INTO job_embeddings (job_id, embedding, raw_text) "
                "VALUES (:jid, :vec::vector, :txt) "
                "ON CONFLICT (job_id) DO UPDATE SET "
                "embedding = EXCLUDED.embedding, raw_text = EXCLUDED.raw_text, updated_at = NOW();",
                jid=str(job_id), vec=vector_literal, txt=raw_text[:120000]
            )
            return True
        finally:
            conn.close()
    except Exception as exc:
        print(f"ERROR: Job pgvector upsert failed: {exc}")
        return False

def save_to_pgvector(candidate_id: str, cv_vector: list[float], parsed_data: dict, masked_cv_text: str) -> bool:
    if not ENABLE_PGVECTOR or not RDS_SECRET_ARN or not cv_vector:
        return False
    try:
        return _upsert_pgvector_with_pg8000(candidate_id, cv_vector, parsed_data, masked_cv_text)
    except Exception as exc:
        print(f"ERROR: pgvector upsert failed via pg8000: {exc}")
        return False

def _search_matching_jobs(
    cv_vector: list[float],
) -> list[dict]:
    """Perform vector search against job_embeddings to find top 10 matches."""
    import pg8000.native
    
    secret = _resolve_rds_credentials()
    host = RDS_PROXY_ENDPOINT or secret["host"]
    dbname = secret["dbname"]
    vector_literal = _build_vector_literal(cv_vector)

    ssl_context = None
    if RDS_SSL_MODE in ["require", "verify-full", "verify-ca"]:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    conn = pg8000.native.Connection(
        user=secret["username"],
        password=secret["password"],
        host=host,
        port=secret["port"],
        database=dbname,
        ssl_context=ssl_context,
        timeout=RDS_CONNECT_TIMEOUT
    )
    
    try:
        # Vector search using L2 distance (<-> operator)
        # We also join with Jobs table to get title and metadata
        query = (
            "SELECT j.id, j.title, (e.embedding <-> :vec::vector) as distance "
            "FROM job_embeddings e "
            "JOIN \"Jobs\" j ON e.job_id = j.\"Id\" "
            "ORDER BY distance ASC "
            "LIMIT 10;"
        )
        results = conn.run(query, vec=vector_literal)
        
        matches = []
        for r in results:
            job_id, title, distance = r
            # Convert L2 distance to a heuristic percentage score
            # L2 distance for normalized vectors ranges from 0 to 2 (squared is 0 to 4).
            # Here we just use a simple linear mapping for demonstration.
            match_score = max(0.0, 100.0 * (1.0 - (float(distance) / 1.5)))
            matches.append({
                "jobId": job_id,
                "jobTitle": title,
                "matchScore": round(match_score, 2),
                "distance": round(float(distance), 4)
            })
        return matches
    except Exception as e:
        print(f"ERROR: Vector search failed: {e}")
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DynamoDB Persistence
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
    top_matches: list | None = None,
) -> None:
    table = dynamodb.Table(DYNAMODB_TABLE)
    now_str = now_iso()
    
    def to_decimal(obj):
        return json.loads(json.dumps(obj or {}), parse_float=Decimal)

    item = {
        "PK": f"CANDIDATE#{profile_id}",
        "SK": f"CV#{file_key}",
        "GSI1PK": "CANDIDATE",
        "GSI1SK": now_str,
        "candidateId": str(profile_id),
        "objectKey": str(file_key),
        "type": "CV_PARSE_RESULT",
        "parseStatus": "SUCCEEDED" if success else "FAILED",
        "updatedAt": now_str,
        "expiresAt": expires_at_epoch(),
        "parsedResult": to_decimal(parsed_data),
        "errorMessage": error_message,
        "scoringDetails": to_decimal(scoring_details),
        "interviewGuide": to_decimal(interview_guide),
        "maskingReport": to_decimal(masking_report),
        "jobSuggestions": to_decimal(top_matches),
    }
    table.put_item(Item=item)

# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    print(f"INFO: VectorPersister started for profile_id={event.get('profile_id')}")
    try:
        profile_id = event.get("profile_id", "unknown")
        file_key = event.get("file_key", "unknown")
        job_id = event.get("job_id", "unknown")
        
        masked_cv_text = event.get("masked_cv_text", "")
        cv_vector = event.get("cv_vector", [])
        parsed_data = event.get("parsed_data", {})
        scoring_details = event.get("scoring_details", {})
        interview_guide = event.get("interview_guide", [])
        masking_report = event.get("masking_report", {})
        
        jd_text = event.get("jd_text", "")
        jd_vector = event.get("jd_vector", [])

        # --- BRANCH A: JD Persistence (Recruiter Flow) ---
        if profile_id == "recruiter":
            print(f"INFO: Processing JD Persistence for jobId={job_id}")
            # For JDs, we use the original text and vector
            jd_saved = save_job_to_pgvector(job_id, jd_vector, jd_text)
            if not jd_saved:
                raise RuntimeError(f"Job persistence failed for {job_id}")
            return {
                "statusCode": 200, 
                "body": json.dumps({"status": "SUCCESS", "type": "JD", "job_id": job_id})
            }
        # Invoke candidate_ranking_engine (async, non-blocking)
        if ENABLE_CANDIDATE_RANKING and CANDIDATE_RANKING_FUNCTION and jd_vector:
            try:
                ranking_payload = {
                    "job_id": job_id,
                    "jd_text": jd_text,
                    "jd_vector": jd_vector,   # float list — already fetched from event
                    "job_title": event.get("job_title", "Unknown Role"),
                    "required_skills": event.get("required_skills", []),
                }
                lambda_client.invoke(
                    FunctionName=CANDIDATE_RANKING_FUNCTION,
                    InvocationType="Event",
                    Payload=json.dumps(ranking_payload, default=str),
                )
                print(f"INFO: candidate_ranking_engine invoked async for job={job_id}")
            except Exception as exc:
                print(f"WARNING: candidate_ranking_engine invocation failed (non-fatal): {exc}")

        # --- BRANCH B: CV Persistence & Matching (Candidate Flow) ---
        # 1. Save to pgvector (RDS)
        pg_saved = save_to_pgvector(profile_id, cv_vector, parsed_data, masked_cv_text)
        if not pg_saved:
             print("WARNING: pgvector persistence failed (non-fatal for global flow)")
        else:
             print("INFO: pgvector save complete")

        # 2. Perform global vector search if embedding exists
        top_matches = []
        if cv_vector:
            print("INFO: Performing global vector search for matches...")
            top_matches = _search_matching_jobs(cv_vector)
            print(f"INFO: Found {len(top_matches)} matches")

        # 3. Save everything to DynamoDB (Hot Store)
        save_to_dynamodb(
            profile_id, file_key, parsed_data, True, "", 
            scoring_details, interview_guide, masking_report, top_matches
        )
        print("INFO: DynamoDB save complete (including matches)")
        
        # Invoke job_suggestion_engine (async, non-blocking)
        if ENABLE_JOB_SUGGESTIONS and JOB_SUGGESTION_FUNCTION and masked_cv_text and cv_vector:
            try:
                suggestion_payload = {
                    "profile_id": profile_id,
                    "file_key": file_key,
                    "masked_cv_text": masked_cv_text[:50000],
                    "cv_vector": cv_vector,   # float list — JSON serializable
                    "parsed_data": parsed_data,
                }
                lambda_client.invoke(
                    FunctionName=JOB_SUGGESTION_FUNCTION,
                    InvocationType="Event",   # Async — don't wait
                    Payload=json.dumps(suggestion_payload, default=str),
                )
                print(f"INFO: job_suggestion_engine invoked async for profile={profile_id}")
            except Exception as exc:
                print(f"WARNING: job_suggestion_engine invocation failed (non-fatal): {exc}")

        return {
            "statusCode": 200, 
            "body": json.dumps({
                "status": "SUCCESS", 
                "profile_id": profile_id,
                "matchCount": len(top_matches)
            })
        }
    except Exception as exc:
        print(f"ERROR: Persistence failed: {exc}")
        return {"statusCode": 500, "body": json.dumps({"status": "FAILED", "error": str(exc)})}
    