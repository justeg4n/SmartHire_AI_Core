import boto3
import json

def evaluate_answer(question, candidate_answer):
    bedrock = boto3.client(service_name='bedrock-runtime')
    model_id = 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    
    # System Prompt with strict JSON schema definition 
    system_prompt = """You are an AI Interview Evaluator. 
    Analyze the candidate's answer to the interviewer's question using the STAR method (Situation, Task, Action, Result).
    
    You MUST output the result strictly as a valid JSON object. 
    Do NOT include any markdown formatting, conversational text, or explanations outside the JSON.
    
    Strict JSON Schema to follow:
    {
      "technical_accuracy": 0, // Integer 0-10
      "depth_of_reasoning": 0, // Integer 0-10
      "communication_clarity": 0, // Integer 0-10
      "star_structure": 0, // Integer 0-10. Did they provide context, actions, and results?
      "justification": "A brief 2-sentence explanation of why these scores were given."
    }"""

    user_message = f"""
    <question>
    {question}
    </question>
    
    <candidate_answer>
    {candidate_answer}
    </candidate_answer>
    """

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 300,
        "temperature": 0.0, # Temperature 0.0 is critical here for consistent, objective grading
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": user_message}
        ]
    }

    print("Evaluating candidate answer via Claude 3.5 Sonnet...")
    response = bedrock.invoke_model(
        modelId=model_id,
        contentType='application/json',
        accept='application/json',
        body=json.dumps(payload)
    )
    
    result = json.loads(response.get('body').read())
    return result['content'][0]['text']

# --- TEST THE FUNCTION ---
if __name__ == "__main__":
    test_question = "How did you optimize the cold starts for your .NET 8 Lambda functions?"
    
    test_answer = "Well, our APIs were running really slow when traffic spiked. So I looked into the AWS docs and decided to implement .NET 8 Native AOT compilation. I updated our SAM template to build the deployment package natively. After deploying, our cold start times dropped from about 2 seconds down to 150 milliseconds, which made our payment gateway much more reliable."
    
    evaluation_json = evaluate_answer(test_question, test_answer)
    
    print("\n--- AI EVALUATION SCORECARD ---")
    print(evaluation_json)
    
    # Safety check to ensure the Backend can parse it
    try:
        json.loads(evaluation_json)
        print("\nSUCCESS: Valid JSON generated.")
    except json.JSONDecodeError:
        print("\nERROR: Output is not valid JSON.")