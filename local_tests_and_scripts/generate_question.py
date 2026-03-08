import boto3
import json

def manage_token_budget(conversation_history):
    """
    Giữ lại tối đa 10 lượt hội thoại gần nhất để tiết kiệm chi phí Token.
    Mỗi lượt gồm 1 câu hỏi của AI và 1 câu trả lời của User.
    """
    if len(conversation_history) > 10:
        return conversation_history[-10:]
    return conversation_history

def generate_interview_question(jd_context, conversation_history, live_code_state):
    bedrock = boto3.client(service_name='bedrock-runtime')
    model_id = 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    
    # Lọc lịch sử hội thoại
    optimized_history = manage_token_budget(conversation_history)
    history_text = "\n".join([f"{msg['role']}: {msg['content']}" for msg in optimized_history])
    
    system_prompt = """You are a Senior Technical Recruiter conducting an interview.
    Your goal is to ask ONE highly relevant, challenging technical follow-up question.
    
    RULES:
    1. Ask exactly ONE question. Keep it conversational (under 3 sentences).
    2. If the candidate is coding, reference their 'Current candidate code' in your question. Mention specific lines or logic if relevant.
    3. DO NOT output JSON. Output only the spoken words.
    """

    user_message = f"""
    <job_description>
    {jd_context}
    </job_description>
    
    <conversation_history>
    {history_text}
    </conversation_history>
    
    <current_candidate_code>
    {live_code_state}
    </current_candidate_code>
    
    Based on the context, history, and their current code, what is your next spoken question?
    """

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 150,
        "temperature": 0.7,
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
    sample_jd = "Looking for a Backend dev skilled in .NET and C#."
    sample_history = [{"role": "Candidate", "content": "I am writing a loop to find the user."}]
    sample_code = """
    foreach (var user in userList) {
        if (user.Id == targetId) return user;
    }
    """
    print(generate_interview_question(sample_jd, sample_history, sample_code))