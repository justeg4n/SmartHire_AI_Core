import boto3
import json
import math
import os
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from botocore.config import Config


retry_config = Config(
    region_name="ap-southeast-1",
    retries={"max_attempts": 10, "mode": "adaptive"},
)

bedrock = boto3.client("bedrock-runtime", config=retry_config)
textract = boto3.client("textract", region_name="ap-southeast-1")
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")

DYNAMODB_JOBS_TABLE = os.environ.get("DYNAMODB_JOBS_TABLE", "SmartHire_Jobs")
CLAUDE_MODEL_ID = os.environ.get("CLAUDE_MODEL_ID") or os.environ.get(
    "BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0"
)
COHERE_MODEL_ID = os.environ.get("COHERE_MODEL_ID", "cohere.embed-english-v3")
CV_BUCKET_NAME = os.environ.get("CV_BUCKET_NAME", "")
DYNAMODB_TABLE = (
    os.environ.get("DYNAMODB_TABLE_NAME")
    or os.environ.get("DYNAMODB_TABLE")
    or "SmartHire_Profiles"
)
PARSE_STATUS_TTL_DAYS = int(os.environ.get("PARSE_STATUS_TTL_DAYS", "30"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def expires_at_epoch() -> int:
    return int((datetime.now(timezone.utc) + timedelta(days=PARSE_STATUS_TTL_DAYS)).timestamp())


def parse_json_if_string(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


def decode_s3_key(raw_key: str) -> str:
    return urllib.parse.unquote_plus(raw_key)


def candidate_id_from_key(object_key: str) -> str:
    parts = object_key.split("/")
    if len(parts) >= 2 and parts[0] == "candidates" and parts[1]:
        return parts[1]
    return "unknown"


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


def get_jd_text_from_db(job_id: str) -> str:
    """Fetch the parsed JD text from DynamoDB using jobId"""
    try:
        table = dynamodb.Table(DYNAMODB_JOBS_TABLE)
        response = table.get_item(Key={"jobId": str(job_id)})
        if "Item" in response and "jdText" in response["Item"]:
            return response["Item"]["jdText"]
    except Exception as e:
        print(f"WARNING: Failed to fetch JD from DB for jobId {job_id}: {str(e)}")
    
    return "Evaluate general technical skills." # Fallback safety

def extract_job_payload(record: dict):
    body = parse_json_if_string(record.get("body", "{}"))

    # Shape A: custom queue payload from Candidate service
    if isinstance(body, dict) and body.get("profileId") and body.get("fileKey"):
        jd_text = body.get("jdText")
        if not jd_text and body.get("jobId"):
            jd_text = get_jd_text_from_db(body.get("jobId"))
        elif not jd_text:
            jd_text = "Evaluate general technical skills."

        return {
            "profile_id": str(body["profileId"]),
            "file_key": str(body["fileKey"]),
            "jd_text": jd_text,
            "bucket": body.get("bucketName") or CV_BUCKET_NAME,
        }

    # Shape B: direct S3 event in SQS
    if isinstance(body, dict):
        records = body.get("Records", [])
        if records and isinstance(records[0], dict):
            s3 = records[0].get("s3", {})
            bucket = s3.get("bucket", {}).get("name")
            raw_key = s3.get("object", {}).get("key")
            if bucket and raw_key:
                file_key = decode_s3_key(raw_key)
                return {
                    "profile_id": candidate_id_from_key(file_key),
                    "file_key": file_key,
                    "jd_text": "Evaluate general technical skills.",
                    "bucket": bucket,
                }

    # Shape C: SNS wrapped S3 event in SQS
    if isinstance(body, dict) and body.get("Message"):
        sns_message = parse_json_if_string(body.get("Message"))
        if isinstance(sns_message, dict):
            records = sns_message.get("Records", [])
            if records and isinstance(records[0], dict):
                s3 = records[0].get("s3", {})
                bucket = s3.get("bucket", {}).get("name")
                raw_key = s3.get("object", {}).get("key")
                if bucket and raw_key:
                    file_key = decode_s3_key(raw_key)
                    return {
                        "profile_id": candidate_id_from_key(file_key),
                        "file_key": file_key,
                        "jd_text": "Evaluate general technical skills.",
                        "bucket": bucket,
                    }

    raise ValueError("SQS message is not recognized. Need custom payload or valid S3 event.")


def get_cohere_embedding(text: str) -> list:
    # Cohere embed-english-v3 enforces max 2048 chars for each input text.
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


def calculate_cosine_similarity(vec1: list, vec2: list) -> float:
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = math.sqrt(sum(a * a for a in vec1))
    magnitude2 = math.sqrt(sum(b * b for b in vec2))

    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0

    return dot_product / (magnitude1 * magnitude2)


def parse_and_evaluate_cv(raw_cv_text: str, jd_text: str, math_match_score: float) -> dict:
    system_prompt = f"""You are an elite Senior Technical Recruiter AI.

CRITICAL INSTRUCTION: A deterministic Machine Learning engine has already compared the semantic vectors of this Candidate's CV against the Job Description.
The official Match Score is exactly {math_match_score}%.
You MUST output this exact score in the \"matching_score\" field. Do not invent your own score.

Using this score as your baseline truth, extract their skills and write a professional summary of their Strengths and Gaps.

You MUST output the result strictly as a valid JSON object.
Do NOT include any conversational text or markdown. Output ONLY raw JSON.

Strict JSON Schema to follow:
{{
  \"seniority_estimate\": \"Junior/Mid/Senior\",
  \"frontend_skills\": [\"array of strings\"],
  \"backend_skills\": [\"array of strings\"],
  \"devops_skills\": [\"array of strings\"],
  \"soft_skills\": [\"array of strings\"],
  \"years_experience\": 0,
  \"matching_score\": {math_match_score},
  \"strengths\": \"Short paragraph explaining why they are a fit.\",
  \"gaps\": \"Short paragraph explaining what required skills they are missing.\"
}}"""

    user_message = f"<job_description>\n{jd_text}\n</job_description>\n<resume>\n{raw_cv_text[:120000]}\n</resume>"
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


def extract_text_from_s3(bucket: str, key: str) -> str:
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
        kwargs = {
            "JobId": job_id,
            "MaxResults": 1000,
        }
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


def save_to_dynamodb(profile_id: str, file_key: str, parsed_data: dict, success: bool, error_message: str = ""):
    table = dynamodb.Table(DYNAMODB_TABLE)
    parsed_data = json.loads(json.dumps(parsed_data), parse_float=Decimal)

    status = "SUCCEEDED" if success else "FAILED"
    item = {
        "candidateId": str(profile_id),
        "objectKey": str(file_key),
        "parseStatus": status,
        "updatedAt": now_iso(),
        "expiresAt": expires_at_epoch(),
        "parsedResult": parsed_data,
        "errorMessage": error_message,
        # Legacy fields kept for backward compatibility in downstream readers.
        "ProfileId": str(profile_id),
        "ProcessSuccess": success,
        "ProcessedAt": int(time.time()),
    }

    table.put_item(Item=item)


def process_record(record: dict):
    payload = extract_job_payload(record)
    profile_id = payload["profile_id"]
    file_key = payload["file_key"]
    jd_text = payload["jd_text"]
    bucket = payload["bucket"]

    if not bucket:
        raise ValueError("Missing bucket name. Provide bucketName in SQS body or CV_BUCKET_NAME env var.")
    if profile_id == "unknown":
        raise ValueError("Cannot derive candidate profile id from message/file key.")

    print(f"INFO: Starting CV processing for ProfileId: {profile_id}, key: {file_key}")

    raw_cv_text = extract_text_from_s3(bucket, file_key)

    print("INFO: Running ML vector embedding")
    cv_vector = get_cohere_embedding(raw_cv_text)
    jd_vector = get_cohere_embedding(jd_text)
    raw_score = calculate_cosine_similarity(cv_vector, jd_vector)
    math_match_score = round(raw_score * 100, 2)
    print(f"INFO: ML Match Score: {math_match_score}%")

    parsed_data = parse_and_evaluate_cv(raw_cv_text, jd_text, math_match_score)
    if "matching_score" not in parsed_data:
        parsed_data["matching_score"] = math_match_score

    save_to_dynamodb(profile_id, file_key, parsed_data, success=True)


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records to process"}

    for record in records:
        profile_id = "unknown"
        file_key = "unknown"
        try:
            payload = extract_job_payload(record)
            profile_id = payload["profile_id"]
            file_key = payload["file_key"]
            process_record(record)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "level": "error",
                        "message": "Candidate CV processing failed",
                        "profileId": profile_id,
                        "fileKey": file_key,
                        "error": str(exc),
                        "bodyPreview": str(record.get("body", ""))[:500],
                    }
                )
            )

            if profile_id != "unknown" and file_key != "unknown":
                save_to_dynamodb(profile_id, file_key, {}, success=False, error_message=str(exc))

            # Re-raise so SQS can retry and move to DLQ if needed.
            raise

    return {"statusCode": 200, "body": "Success"}