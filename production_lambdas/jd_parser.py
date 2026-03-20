import boto3
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone
from botocore.config import Config

retry_config = Config(region_name="ap-southeast-1", retries={"max_attempts": 10, "mode": "adaptive"})

bedrock = boto3.client("bedrock-runtime", config=retry_config)
textract = boto3.client("textract", region_name="ap-southeast-1")
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")

CLAUDE_MODEL_ID = os.environ.get("CLAUDE_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
DYNAMODB_JOBS_TABLE = os.environ.get("DYNAMODB_JOBS_TABLE", "SmartHire_Jobs")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
            raise RuntimeError("Textract JD job failed")
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

def structure_jd_with_claude(raw_text: str) -> dict:
    system_prompt = """You are an expert HR AI. 
Extract and structure the following raw Job Description text.
Output MUST be valid JSON with this exact schema:
{
  "job_title": "String",
  "required_skills": ["array of key skills"],
  "cleaned_jd_text": "A clean, well-formatted paragraph summarizing the role and requirements."
}"""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "temperature": 0.0,
        "system": system_prompt,
        "messages": [{"role": "user", "content": raw_text[:100000]}]
    }

    response = bedrock.invoke_model(
        modelId=CLAUDE_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    
    response_data = json.loads(response.get("body").read())
    model_text = response_data.get("content", [{}])[0].get("text", "{}")
    
    try:
        return json.loads(model_text)
    except Exception:
        return {"job_title": "Unknown", "required_skills": [], "cleaned_jd_text": raw_text}

def lambda_handler(event, context):
    for record in event.get("Records", []):
        try:
            body = json.loads(record.get("body", "{}"))
            job_id = body.get("jobId")
            bucket = body.get("bucketName")
            file_key = body.get("fileKey")
            
            if not all([job_id, bucket, file_key]):
                raise ValueError("Missing jobId, bucketName, or fileKey in payload")

            print(f"INFO: Parsing JD for JobId: {job_id}")
            
            raw_jd_text = extract_text_from_s3(bucket, file_key)
            structured_jd = structure_jd_with_claude(raw_jd_text)
            
            # Save to DynamoDB
            table = dynamodb.Table(DYNAMODB_JOBS_TABLE)
            table.put_item(Item={
                "jobId": str(job_id),
                "originalFileKey": str(file_key),
                "jobTitle": structured_jd.get("job_title"),
                "requiredSkills": structured_jd.get("required_skills"),
                "jdText": structured_jd.get("cleaned_jd_text"),
                "parseStatus": "SUCCEEDED",
                "updatedAt": now_iso()
            })
            
            print(f"SUCCESS: JD {job_id} saved to DynamoDB.")
            
        except Exception as exc:
            print(f"ERROR processing JD: {str(exc)}")
            raise

    return {"statusCode": 200, "body": "Success"}