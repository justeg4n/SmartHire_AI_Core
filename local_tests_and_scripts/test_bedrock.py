import boto3
import json

def test_claude_connection():
    # Initialize the Bedrock runtime client
    bedrock_client = boto3.client(service_name='bedrock-runtime')
    
    # Model ID for Claude 3.5 Sonnet
    model_id = 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    
    # Construct the payload
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 512,
        "temperature": 0.5,
        "messages": [
            {
                "role": "user",
                "content": "You are an AI interviewer. Generate one technical interview question for a Junior .NET Backend Developer."
            }
        ]
    }
    
    try:
        response = bedrock_client.invoke_model(
            modelId=model_id,
            contentType='application/json',
            accept='application/json',
            body=json.dumps(payload)
        )
        
        # Parse and print the response
        response_body = json.loads(response.get('body').read())
        print("Connection Successful! AI Response:")
        print(response_body['content'][0]['text'])
        
    except Exception as e:
        print(f"Error invoking Bedrock: {e}")

if __name__ == "__main__":
    test_claude_connection()