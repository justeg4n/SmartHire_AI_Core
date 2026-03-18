import boto3
import json
import urllib.request
import os
import time
import math
from botocore.config import Config

# 1. AWS CLIENT CONFIGURATION
retry_config = Config(
    region_name='ap-southeast-1', 
    retries={'max_attempts': 10, 'mode': 'adaptive'}
)

bedrock = boto3.client('bedrock-runtime', config=retry_config)
textract = boto3.client('textract', region_name='ap-southeast-1')

CLAUDE_MODEL_ID = 'apac.anthropic.claude-3-5-sonnet-20241022-v2:0'
COHERE_MODEL_ID = 'cohere.embed-english-v3'

# 2. MATH FUNCTIONS: VECTOR & COSINE SIMILARITY
def get_cohere_embedding(text: str) -> list:
    """Converts text into a 1024-dimensional vector array using Cohere."""
    safe_text = text[:8000] # Cohere safe limit
    
    payload = {
        "texts": [safe_text],
        "input_type": "search_document", 
        "truncate": "END" 
    }
    
    response = bedrock.invoke_model(
        modelId=COHERE_MODEL_ID,
        contentType='application/json',
        accept='application/json',
        body=json.dumps(payload)
    )
    
    response_body = json.loads(response.get('body').read())
    return response_body['embeddings'][0]

def calculate_cosine_similarity(vec1: list, vec2: list) -> float:
    """Calculates the angle between 2 vectors. Returns a float from 0.0 to 1.0"""
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = math.sqrt(sum(a * a for a in vec1))
    magnitude2 = math.sqrt(sum(b * b for b in vec2))
    
    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0
        
    return dot_product / (magnitude1 * magnitude2)

# 3. CORE AI: Parse CV with Claude
def parse_and_evaluate_cv(raw_cv_text, jd_text, math_match_score):
    """
    Claude receives the pre-calculated Math score to act as its baseline truth.
    """
    system_prompt = f"""You are an elite Senior Technical Recruiter AI.
    
    CRITICAL INSTRUCTION: A deterministic Machine Learning engine has already compared the semantic vectors of this Candidate's CV against the Job Description. 
    The official Match Score is exactly {math_match_score}%. 
    You MUST output this exact score in the "matching_score" field. Do not invent your own score.
    
    Using this score as your baseline truth, extract their skills and write a professional summary of their Strengths and Gaps.
    
    You MUST output the result strictly as a valid JSON object. 
    Do NOT include any conversational text or markdown. Output ONLY raw JSON.
    
    Strict JSON Schema to follow:
    {{
      "seniority_estimate": "Junior/Mid/Senior",
      "frontend_skills": ["array of strings"],
      "backend_skills": ["array of strings"],
      "devops_skills": ["array of strings"],
      "soft_skills": ["array of strings"],
      "years_experience": 0,
      "matching_score": {math_match_score},
      "strengths": "Short paragraph explaining why they are a {math_match_score}% fit.",
      "gaps": "Short paragraph explaining what required skills they are missing."
    }}"""

    user_message = f"<job_description>\n{jd_text}\n</job_description>\n<resume>\n{raw_cv_text}\n</resume>"

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "temperature": 0.0,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}]
    }

    response = bedrock.invoke_model(
        modelId=CLAUDE_MODEL_ID,
        contentType='application/json',
        accept='application/json',
        body=json.dumps(payload)
    )
    
    return json.loads(response.get('body').read())['content'][0]['text']

# 4. TEXTRACT: Read PDF/Image from S3
def extract_text_from_s3(bucket, key):
    response = textract.start_document_text_detection(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
    )
    job_id = response['JobId']
    
    while True:
        result = textract.get_document_text_detection(JobId=job_id)
        if result['JobStatus'] == 'SUCCEEDED':
            break
        elif result['JobStatus'] == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(2)
    
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

# 5. BACKEND API CALLER
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
        api_url, data=data,
        headers={"Content-Type": "application/json", "ngrok-skip-browser-warning": "true"},
        method="POST"
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"Backend API called successfully for profile {profile_id}")
    except Exception as e:
        print(f"Backend API call failed: {str(e)}")

# 6. LAMBDA HANDLER
def lambda_handler(event, context):
    profile_id = None
    try:
        sqs_body = json.loads(event['Records'][0]['body'])
        profile_id = sqs_body['profileId']
        file_key = sqs_body['fileKey']
        jd_text = sqs_body.get('jdText', 'Evaluate general technical skills.')
        
        print(f"Starting CV processing | ProfileId: {profile_id}")
        
        # Step 1: Extract Text
        raw_cv_text = extract_text_from_s3("hirelo-cv-storage", file_key)
        
        # Step 2: Math Score (Vector Similarity)
        print("Running ML Vector Embedding...")
        cv_vector = get_cohere_embedding(raw_cv_text)
        jd_vector = get_cohere_embedding(jd_text)
        
        raw_score = calculate_cosine_similarity(cv_vector, jd_vector)
        math_match_score = round(raw_score * 100, 2)
        print(f"ML Match Score: {math_match_score}%")
        
        # Step 3: Claude Contextual Analysis
        parsed_json_string = parse_and_evaluate_cv(raw_cv_text, jd_text, math_match_score)
        parsed_data = json.loads(parsed_json_string)
        
        # Step 4: Post to Backend
        call_backend_api(profile_id, parsed_data, success=True)
        return {'statusCode': 200, 'body': 'Success'}
        
    except Exception as e:
        print(f"System Error: {str(e)}")
        if profile_id:
            call_backend_api(profile_id, {}, success=False)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}