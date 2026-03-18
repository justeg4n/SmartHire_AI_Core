import boto3
import json

# Ensure you are using the correct region where you want to operate
bedrock = boto3.client('bedrock-runtime', region_name='ap-southeast-1')

def auto_unlock_cohere():
    print("Attempting to invoke Cohere to trigger AWS Auto-Enable...")
    
    payload = {
        "texts": ["This is a test to unlock the model."],
        "input_type": "search_document", 
        "truncate": "END" 
    }
    
    try:
        response = bedrock.invoke_model(
            modelId='cohere.embed-english-v3', 
            contentType='application/json',
            accept='application/json',
            body=json.dumps(payload)
        )
        print(" SUCCESS! The model is now unlocked and enabled for your entire AWS account.")
        
    except Exception as e:
        print("\n FAILED TO UNLOCK. AWS returned this error:")
        print(str(e))
        print("\nIf the error mentions 'Marketplace' or 'EULA', your current AWS credentials do not have permission to accept the terms. Please run this script using the Root User's Access Keys.")

if __name__ == "__main__":
    auto_unlock_cohere()