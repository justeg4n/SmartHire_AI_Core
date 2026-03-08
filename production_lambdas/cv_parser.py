import boto3
import json

#1. HÀM CORE AI: Nơi chứa logic Prompt Engineering
def parse_cv_with_bedrock(raw_cv_text):
    bedrock = boto3.client(service_name='bedrock-runtime')
    model_id = 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    
    system_prompt = """You are an expert technical recruiter AI evaluating a candidate's resume.
    Your task is to extract the candidate's skills and experience.
    
    You MUST output the result strictly as a valid JSON object. 
    Do NOT include any conversational text or markdown formatting. Output ONLY the raw JSON.
    
    Strict JSON Schema to follow:
    {
      "frontend_skills": ["array of strings"],
      "backend_skills": ["array of strings"],
      "devops_skills": ["array of strings"],
      "soft_skills": ["array of strings"],
      "years_experience": 0,
      "seniority_estimate": "Junior/Mid/Senior"
    }"""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "temperature": 0.0,
        "system": system_prompt,
        "messages": [
            {
                "role": "user",
                "content": f"Please analyze this resume text:\n\n<resume>\n{raw_cv_text}\n</resume>"
            }
        ]
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        contentType='application/json',
        accept='application/json',
        body=json.dumps(payload)
    )
    
    response_body = json.loads(response.get('body').read())
    return response_body['content'][0]['text']

#2. HÀM LAMBDA HANDLER: Điểm đón Event từ AWS (S3 Trigger)
def lambda_handler(event, context):
    """
    Hàm này tự động chạy khi có 1 file CV mới được upload lên S3.
    Nó nhận 'event' chứa thông tin file, đọc text bằng Textract, và gửi cho Bedrock.
    """
    try:
        #1. Trích xuất tên bucket và tên file từ Event JSON của S3
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        print(f"Bắt đầu xử lý file CV: {file_key} từ bucket: {bucket_name}")
        
        #2. Gọi Textract để đọc chữ từ file PDF/Docx trên S3
        textract = boto3.client('textract')
        response = textract.detect_document_text(
            Document={'S3Object': {'Bucket': bucket_name, 'Name': file_key}}
        )
        
        #Nối tất cả các dòng text Textract đọc được thành một string dài
        raw_cv_text = " ".join([item["Text"] for item in response["Blocks"] if item["BlockType"] == "LINE"])
        
        #3. Chuyển text thô cho AI xử lý
        parsed_json_string = parse_cv_with_bedrock(raw_cv_text) 
        
        #4. Trả về kết quả cho Backend (API Gateway hoặc bước tiếp theo để lưu vào DynamoDB)
        return {
            'statusCode': 200,
            'body': json.loads(parsed_json_string)
        }
        
    except Exception as e:
        print(f"Lỗi hệ thống: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }