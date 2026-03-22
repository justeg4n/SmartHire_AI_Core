import boto3
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone
from botocore.config import Config

retry_config = Config(region_name="ap-southeast-1", retries={"max_attempts": 10, "mode": "adaptive"})

s3_client = boto3.client("s3", region_name="ap-southeast-1")
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
            raise RuntimeError("Textract JD OCR job failed")
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

def get_job_id_from_s3_metadata(bucket: str, key: str) -> str:
    """Attempts to read the jobId from S3 object metadata or filename."""
    try:
        # Check if Backend attached metadata during upload
        response = s3_client.head_object(Bucket=bucket, Key=key)
        metadata = response.get('Metadata', {})
        if 'jobid' in metadata:
            return metadata['jobid']
    except Exception as e:
        print(f"WARNING: Could not fetch metadata for {key}: {str(e)}")
    
    # Fallback: Extract jobId from filename (e.g., 'jds/job-123.pdf' -> 'job-123')
    filename = key.split('/')[-1]
    return filename.rsplit('.', 1)[0]

def lambda_handler(event, context):
    jobs_to_process = []

    # Scenario A: S3 Event Trigger (Direct from S3 Bucket)
    if "Records" in event and event["Records"][0].get("eventSource") == "aws:s3":
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            raw_key = record["s3"]["object"]["key"]
            file_key = urllib.parse.unquote_plus(raw_key)
            job_id = get_job_id_from_s3_metadata(bucket, file_key)
            jobs_to_process.append({"jobId": job_id, "bucketName": bucket, "fileKey": file_key})

    # Scenario B: Direct Lambda Invoke from .NET Backend (Synchronous API call)
    elif "jobId" in event and "fileKey" in event:
        jobs_to_process.append({
            "jobId": event["jobId"],
            "bucketName": event.get("bucketName", os.environ.get("CV_BUCKET_NAME")),
            "fileKey": event["fileKey"]
        })
    else:
        return {"statusCode": 400, "body": "Unrecognized event format. Need S3 event or direct JSON payload."}

    # Process all identified jobs
    for job in jobs_to_process:
        job_id = job["jobId"]
        bucket = job["bucketName"]
        file_key = job["fileKey"]

        print(f"INFO: Parsing JD for JobId: {job_id} from s3://{bucket}/{file_key}")
        
        try:
            raw_jd_text = extract_text_from_s3(bucket, file_key)
            structured_jd = structure_jd_with_claude(raw_jd_text)
            
            # Save directly to DynamoDB
            table = dynamodb.Table(DYNAMODB_JOBS_TABLE)
            table.put_item(Item={
                "jobId": str(job_id),
                "originalFileKey": str(file_key),
                "jobTitle": structured_jd.get("job_title", "Unknown"),
                "requiredSkills": structured_jd.get("required_skills", []),
                "jdText": structured_jd.get("cleaned_jd_text", raw_jd_text),
                "parseStatus": "SUCCEEDED",
                "updatedAt": now_iso()
            })
            print(f"SUCCESS: JD {job_id} saved to DynamoDB.")
            
        except Exception as exc:
            print(f"ERROR: Failed processing JD {job_id}: {str(exc)}")
            # Optional: Save FAILED status to DynamoDB here if needed
            raise

    return {"statusCode": 200, "body": "Successfully processed JD upload."}