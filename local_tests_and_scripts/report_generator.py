import boto3
import json

def generate_executive_summary(star_scores, emotion_metrics, code_result):
    bedrock = boto3.client(service_name='bedrock-runtime')
    model_id = 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    
    system_prompt = """You are an Expert Technical Hiring Manager.
    Write a 300-word executive summary evaluating the candidate based on the provided data.
    Highlight their technical strengths, communication skills, and behavior under stress.
    Conclude with a definitive HIRE or NO HIRE recommendation.
    Output ONLY the narrative text, no JSON."""

    user_message = f"""
    <star_evaluation_scores>
    {json.dumps(star_scores)}
    </star_evaluation_scores>
    
    <emotion_and_attention_metrics>
    {json.dumps(emotion_metrics)}
    </emotion_and_attention_metrics>
    
    <coding_sandbox_results>
    {json.dumps(code_result)}
    </coding_sandbox_results>
    
    Please write the executive summary.
    """

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 400,
        "temperature": 0.2, # Giữ ở mức thấp để AI viết báo cáo khách quan, không phóng đại
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}]
    }

    response = bedrock.invoke_model(
        modelId=model_id,
        contentType='application/json',
        accept='application/json',
        body=json.dumps(payload)
    )
    
    result = json.loads(response.get('body').read())
    return result['content'][0]['text']

# --- TEST ---
if __name__ == "__main__":
    # Dữ liệu giả lập từ Step Functions truyền vào
    mock_star = {"technical_accuracy": 8, "communication": 7}
    mock_emotion = {"percent_time_calm": 85.0, "overall_stress_level": 10.0}
    mock_code = {"passed_tests": True, "runtime_ms": 120}
    
    print("\n--- EXECUTIVE SUMMARY ---")
    print(generate_executive_summary(mock_star, mock_emotion, mock_code))