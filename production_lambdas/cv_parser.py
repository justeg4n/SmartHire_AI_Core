import boto3
import json
import urllib.request
import os
import time
from botocore.config import Config

# 1. CẤU HÌNH AWS CLIENTS (Chuẩn Singapore)
retry_config = Config(
    region_name='ap-southeast-1', 
    retries={
        'max_attempts': 10,
        'mode': 'adaptive'
    }
)

bedrock = boto3.client('bedrock-runtime', config=retry_config)
textract = boto3.client('textract', region_name='ap-southeast-1')

MODEL_ID = 'apac.anthropic.claude-3-5-sonnet-20241022-v2:0'

# 2. HÀM CORE AI: Phân tích CV & So sánh JD
def parse_and_evaluate_cv(raw_cv_text, jd_text):
    system_prompt = """You are an elite Senior Technical Recruiter AI evaluating a candidate's resume against a specific Job Description (JD).
    Your task is to extract their skills AND evaluate their fit for the role.
    
    You MUST output the result strictly as a valid JSON object. 
    Do NOT include any conversational text or markdown formatting. Output ONLY the raw JSON.
    
    Strict JSON Schema to follow:
    {
      "seniority_estimate": "Junior/Mid/Senior",
      "frontend_skills": ["array of strings"],
      "backend_skills": ["array of strings"],
      "devops_skills": ["array of strings"],
      "soft_skills": ["array of strings"],
      "years_experience": 0,
      "matching_score": 0,
      "strengths": "Short paragraph explaining why they are a good fit for this specific JD.",
      "gaps": "Short paragraph explaining what required skills from the JD they are missing."
    }"""

    # Gộp cả CV và JD vào một prompt
    user_message = f"""
    <job_description>\n{jd_text}\n</job_description>
    <resume>\n{raw_cv_text}\n</resume>
    
    Please analyze this resume and evaluate it against the job description.
    """

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "temperature": 0.0,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}]
    }

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        contentType='application/json',
        accept='application/json',
        body=json.dumps(payload)
    )
    
    response_body = json.loads(response.get('body').read())
    return response_body['content'][0]['text']

# 3. HÀM ĐỌC PDF/IMAGE TỪ S3 BẰNG TEXTRACT
def extract_text_from_s3(bucket, key):
    response = textract.start_document_text_detection(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
    )
    job_id = response['JobId']
    
    # Chờ Textract xử lý xong
    while True:
        result = textract.get_document_text_detection(JobId=job_id)
        status = result['JobStatus']
        if status == 'SUCCEEDED':
            break
        elif status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(2)
    
    # Gom chữ từ TẤT CẢ các trang (Xử lý Pagination)
    pages_text = []
    while True:
        for block in result["Blocks"]:
            if block["BlockType"] == "LINE":
                pages_text.append(block["Text"])
                
        next_token = result.get('NextToken')
        if not next_token:
            break
        result = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
        
    return " ".join(pages_text)

# 4. HÀM GỌI BACKEND API ĐỂ LƯU KẾT QUẢ
def call_backend_api(profile_id, parsed_data, success=True):
    backend_url = os.environ.get('BACKEND_URL', 'http://localhost:5161')
    api_url = f"{backend_url}/api/cv/parsed-result"
    
    payload = {
        "profileId": profile_id,
        "success": success,
        "seniorityEstimate": parsed_data.get("seniority_estimate", ""),
        "frontendSkills": parsed_data.get("frontend_skills", []),
        "backendSkills": parsed_data.get("backend_skills", []),
        "devopsSkills": parsed_data.get("devops_skills", []),
        "softSkills": parsed_data.get("soft_skills", []),
        "yearsExperience": parsed_data.get("years_experience", 0),
        "matchingScore": parsed_data.get("matching_score", 0),
        "strengths": parsed_data.get("strengths", ""),
        "gaps": parsed_data.get("gaps", "")
    }
    
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "ngrok-skip-browser-warning": "true"
        },
        method="POST"
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"Backend API called successfully for profile {profile_id}")
    except Exception as e:
        print(f"Backend API call failed: {str(e)}")

# 5. LAMBDA HANDLER (Điểm chạm sự kiện SQS)
def lambda_handler(event, context):
    profile_id = None
    try:
        # 1. Đọc message từ SQS do Backend gửi sang
        sqs_body = json.loads(event['Records'][0]['body'])
        profile_id = sqs_body['profileId']
        file_key = sqs_body['fileKey']
        
        # Nhận jdText từ Backend. Nếu Backend quên gửi, để chuỗi trống để AI tự phân tích CV chay.
        jd_text = sqs_body.get('jdText', 'No specific job description provided. Evaluate general technical skills.')
        
        bucket_name = "hirelo-cv-storage"
        
        print(f"Bắt đầu xử lý CV | ProfileId: {profile_id} | FileKey: {file_key}")
        
        # 2. Textract đọc text từ S3
        raw_cv_text = extract_text_from_s3(bucket_name, file_key)
        print(f"Textract extracted {len(raw_cv_text)} characters")
        
        # 3. Bedrock parse CV & Match JD
        parsed_json_string = parse_and_evaluate_cv(raw_cv_text, jd_text)
        parsed_data = json.loads(parsed_json_string)
        print(f"Bedrock parsed: {json.dumps(parsed_data)}")
        
        # 4. Gọi API .NET lưu kết quả
        call_backend_api(profile_id, parsed_data, success=True)
        
        return {'statusCode': 200, 'body': 'Success'}
        
    except Exception as e:
        print(f"Lỗi hệ thống: {str(e)}")
        if profile_id:
            call_backend_api(profile_id, {}, success=False)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}