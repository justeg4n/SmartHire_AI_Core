# =============================================================================
# ingestion_trigger.py — SmartHire AI | IngestionTrigger Lambda
#
# Responsibility: Validate S3 file + route to VectorOps.
#
# Architecture role: Lambda 1 ("Starter")
#   SQS → IngestionTrigger → (Direct Invoke vector_ops)
#   Future: SQS → IngestionTrigger → Step Functions StartExecution
# =============================================================================

import json
import os
import urllib.parse

import boto3
from botocore.config import Config


# ---------------------------------------------------------------------------
# AWS Clients
# ---------------------------------------------------------------------------

retry_config = Config(
    region_name="ap-southeast-1",
    retries={"max_attempts": 5, "mode": "adaptive"},
)

s3 = boto3.client("s3", config=retry_config)
lambda_client = boto3.client("lambda", config=retry_config)
sfn = boto3.client("stepfunctions", config=retry_config)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEXT_PROCESSOR_FUNCTION = os.environ.get("VECTOR_OPS_FUNCTION_NAME", "")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")

# File validation
MAX_FILE_SIZE_BYTES = int(os.environ.get("MAX_FILE_SIZE_BYTES", str(50 * 1024 * 1024)))  # 50MB
ALLOWED_CONTENT_TYPES = {"application/pdf"}
ALLOWED_EXTENSIONS = {".pdf"}

CV_BUCKET_NAME = os.environ.get("CV_BUCKET_NAME", "")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME", "SmartHire_ApplicationTracking")

dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def decode_s3_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def extract_job_id_and_candidate_from_key(object_key: str) -> tuple[str, str | None]:
    """
    Extract IDs from S3 key patterns:
    CV: candidates/{candidateId}/{jobId}/filename.pdf (specific apply)
    CV: candidates/{candidateId}/filename.pdf (global upload)
    JD: jobs/{jobId}/filename.pdf
    """
    parts = object_key.split("/")
    try:
        if "candidates" in parts:
            idx = parts.index("candidates")
            candidate_id = parts[idx + 1] if len(parts) > idx + 1 else "unknown"
            # If the next part is a job ID (based on depth), return it; else None
            if len(parts) > idx + 3:  # candidates/ID/JOBID/file.pdf
                job_id = parts[idx + 2]
                return candidate_id, job_id
            return candidate_id, None
        elif "jobs" in parts:
            idx = parts.index("jobs")
            job_id = parts[idx + 1] if len(parts) > idx + 1 else "unknown"
            return "recruiter", job_id
        return "unknown", None
    except (ValueError, IndexError):
        return "unknown", None


def parse_json_if_string(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def get_jd_data_from_db(job_id: str | None) -> dict:
    """Fetch JD text, title, required skills, and cached vector from DynamoDB."""
    default_data = {
        "jdText": None,
        "jobTitle": "Global Evaluation",
        "requiredSkills": [],
        "jdVector": None,
    }
    if not job_id or job_id == "unknown":
        return default_data

    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        # Single Table Design: PK=JOB#<id>, SK=METADATA
        response = table.get_item(Key={"PK": f"JOB#{job_id}", "SK": "METADATA"})
        if "Item" in response:
            item = response["Item"]
            raw_vector = item.get("jdVector")
            jd_vector = [float(v) for v in raw_vector] if raw_vector else None
            return {
                "jdText": item.get("jdText"),
                "jobTitle": item.get("jobTitle", "Unknown Job"),
                "requiredSkills": item.get("requiredSkills", []),
                "jdVector": jd_vector,
            }
    except Exception as e:
        print(f"WARNING: Failed to fetch JD for jobId {job_id}: {e}")
    return default_data


# ---------------------------------------------------------------------------
# SQS Payload Extraction
# ---------------------------------------------------------------------------

def extract_job_payload(record: dict) -> dict:
    """Parse SQS record body into standardized payload dict."""
    body = parse_json_if_string(record.get("body", "{}"))

    # Shape A: custom queue payload from Candidate service
    if isinstance(body, dict) and body.get("profileId") and body.get("fileKey"):
        job_id = body.get("jobId")
        jd_data = get_jd_data_from_db(job_id)
        if body.get("jdText"):
            jd_data["jdText"] = body.get("jdText")
        return {
            "profile_id": str(body["profileId"]),
            "job_id": str(job_id) if job_id else None,
            "file_key": str(body["fileKey"]),
            "jd_text": jd_data["jdText"],
            "job_title": jd_data["jobTitle"],
            "required_skills": jd_data["requiredSkills"],
            "jd_vector": jd_data["jdVector"],
            "bucket": body.get("bucketName") or CV_BUCKET_NAME,
        }

    # Shape B: direct S3 event in SQS
    if isinstance(body, dict):
        records = body.get("Records", [])
        if records and isinstance(records[0], dict):
            s3_event = records[0].get("s3", {})
            bucket = s3_event.get("bucket", {}).get("name")
            raw_key = s3_event.get("object", {}).get("key")
            if bucket and raw_key:
                file_key = decode_s3_key(raw_key)
                candidate_id, job_id = extract_job_id_and_candidate_from_key(file_key)
                jd_data = get_jd_data_from_db(job_id)
                return {
                    "profile_id": candidate_id,
                    "job_id": job_id,
                    "file_key": file_key,
                    "jd_text": jd_data["jdText"],
                    "job_title": jd_data["jobTitle"],
                    "required_skills": jd_data["requiredSkills"],
                    "jd_vector": jd_data["jdVector"],
                    "bucket": bucket,
                }

    # Shape C: SNS wrapped S3 event in SQS
    if isinstance(body, dict) and body.get("Message"):
        sns_message = parse_json_if_string(body.get("Message"))
        if isinstance(sns_message, dict):
            records = sns_message.get("Records", [])
            if records and isinstance(records[0], dict):
                s3_event = records[0].get("s3", {})
                bucket = s3_event.get("bucket", {}).get("name")
                raw_key = s3_event.get("object", {}).get("key")
                if bucket and raw_key:
                    file_key = decode_s3_key(raw_key)
                    candidate_id, job_id = extract_job_id_and_candidate_from_key(file_key)
                    jd_data = get_jd_data_from_db(job_id)
                    return {
                        "profile_id": candidate_id,
                        "job_id": job_id,
                        "file_key": file_key,
                        "jd_text": jd_data["jdText"],
                        "job_title": jd_data["jobTitle"],
                        "required_skills": jd_data["requiredSkills"],
                        "jd_vector": jd_data["jdVector"],
                        "bucket": bucket,
                    }

    raise ValueError("SQS message not recognized. Need custom payload or S3 event.")


# ---------------------------------------------------------------------------
# File Validation
# ---------------------------------------------------------------------------

def validate_file(bucket: str, file_key: str) -> dict:
    """
    Validate file exists, is within size limits, and is a PDF.

    Returns validation result dict with 'valid' bool and 'reason' string.
    """
    # Check extension
    _, ext = os.path.splitext(file_key.lower())
    if ext not in ALLOWED_EXTENSIONS:
        return {"valid": False, "reason": f"Invalid file extension: {ext}"}

    # Check file exists and size via HeadObject
    try:
        head = s3.head_object(Bucket=bucket, Key=file_key)
        file_size = head.get("ContentLength", 0)
        content_type = head.get("ContentType", "")

        if file_size == 0:
            return {"valid": False, "reason": "File is empty (0 bytes)"}
        if file_size > MAX_FILE_SIZE_BYTES:
            return {
                "valid": False,
                "reason": f"File too large: {file_size} bytes (max {MAX_FILE_SIZE_BYTES})",
            }

        print(
            f"INFO: File validated — size={file_size}, "
            f"type={content_type}, key={file_key}"
        )
        return {"valid": True, "reason": "OK", "file_size": file_size}

    except s3.exceptions.NoSuchKey:
        return {"valid": False, "reason": f"File not found: s3://{bucket}/{file_key}"}
    except Exception as exc:
        return {"valid": False, "reason": f"HeadObject failed: {exc}"}


# ---------------------------------------------------------------------------
# Processing Router
# ---------------------------------------------------------------------------

def invoke_vector_ops(payload: dict) -> dict:
    """
    Invoke vector_ops Lambda (asynchronous).

    In Stage 4, this will be replaced with sfn.start_execution().
    """
    if not TEXT_PROCESSOR_FUNCTION:
        raise ValueError(
            "VECTOR_OPS_FUNCTION_NAME not configured. "
            "Set environment variable to the vector_ops Lambda name."
        )

    # Serialize jd_vector as list (avoid Decimal issues)
    invoke_payload = {
        "profile_id": payload["profile_id"],
        "job_id": payload["job_id"],
        "file_key": payload["file_key"],
        "jd_text": payload["jd_text"],
        "job_title": payload["job_title"],
        "required_skills": payload["required_skills"],
        "jd_vector": payload["jd_vector"],
        "bucket": payload["bucket"],
    }

    print(f"INFO: Invoking vector_ops for profile={payload['profile_id']}")

    response = lambda_client.invoke(
        FunctionName=TEXT_PROCESSOR_FUNCTION,
        InvocationType="Event",  # Async — don't wait for completion
        Payload=json.dumps(invoke_payload),
    )

    status_code = response.get("StatusCode", 0)
    if status_code not in (200, 202):
        raise RuntimeError(
            f"vector_ops invocation failed: HTTP {status_code}"
        )

    print(f"INFO: vector_ops invoked successfully (async, HTTP {status_code})")
    return {"invoked": True, "status_code": status_code}


def start_processing(payload: dict) -> dict:
    """
    Start Step Functions execution when configured.

    Falls back to direct vector_ops invocation for backward compatibility.
    """
    if STATE_MACHINE_ARN:
        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(payload),
        )
        execution_arn = response.get("executionArn", "")
        print(f"INFO: Step Functions execution started: {execution_arn}")
        return {"started": True, "execution_arn": execution_arn}

    return invoke_vector_ops(payload)


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Process SQS batch: validate each file and route to text_processor.

    Failures are re-raised to let SQS retry → DLQ after max attempts.
    """
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records to process"}

    print(f"INFO: IngestionTrigger received {len(records)} record(s)")

    for record in records:
        profile_id = "unknown"
        file_key = "unknown"

        try:
            # 1. Parse SQS payload
            payload = extract_job_payload(record)
            profile_id = payload["profile_id"]
            file_key = payload["file_key"]
            bucket = payload["bucket"]

            if not bucket:
                raise ValueError("Missing bucket name")
            if profile_id == "unknown":
                raise ValueError("Cannot derive candidate profile id")

            print(
                f"INFO: Processing file — "
                f"profile={profile_id}, job={payload['job_id']}, key={file_key}"
            )

            # 2. Validate file
            validation = validate_file(bucket, file_key)
            if not validation["valid"]:
                raise ValueError(f"File validation failed: {validation['reason']}")

            # 3. Route to Step Functions (fallback: direct vector_ops)
            start_processing(payload)

        except Exception as exc:
            print(
                json.dumps({
                    "level": "error",
                    "message": "IngestionTrigger processing failed",
                    "profileId": profile_id,
                    "fileKey": file_key,
                    "error": str(exc),
                })
            )
            raise  # Re-raise for SQS retry → DLQ

    return {"statusCode": 200, "body": f"Processed {len(records)} record(s)"}
