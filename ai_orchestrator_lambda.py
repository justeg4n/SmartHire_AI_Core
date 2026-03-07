import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
# Prerequisite: pip install aws-xray-sdk
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing (measures latency for the <1.5s NFR)
patch_all()

# 1. SET UP PROFESSIONAL LOGGING FOR CLOUDWATCH
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 2. COLD START OPTIMIZATION: Initialize Clients in the Global Scope
bedrock_client = boto3.client('bedrock-runtime')
MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')
MAX_TOKENS = int(os.environ.get('MAX_TOKENS', '150'))

def manage_token_budget(history: list) -> list:
    """Keep the last 10 conversation turns to prevent Context Window overflow."""
    return history[-10:] if len(history) > 10 else history

@xray_recorder.capture('GenerateInterviewQuestion')
def lambda_handler(event, context):
    """
    Professional AWS Lambda Handler for the AI Orchestrator.
    Expected event payload: { "jd_context": "...", "history": [...], "code_state": "..." }
    """
    logger.info("Initializing AI Orchestrator Lambda Handler")
    
    try:
        # Safely extract data using .get()
        jd_context = event.get('jd_context', '')
        history = event.get('history', [])
        code_state = event.get('code_state', 'No code written yet.')
        
        optimized_history = manage_token_budget(history)
        history_text = "\n".join([f"{msg.get('role', 'Unknown')}: {msg.get('content', '')}" for msg in optimized_history])
        
        system_prompt = """You are a Senior Technical Recruiter AI for SmartHire.
        RULES:
        1. Ask ONE highly relevant, challenging technical follow-up question (under 3 sentences).
        2. Strictly reference the 'current_candidate_code' logic in your question if it is not empty.
        3. DO NOT output JSON. Output ONLY the spoken words."""

        user_message = f"""
        <job_description>\n{jd_context}\n</job_description>
        <conversation_history>\n{history_text}\n</conversation_history>
        <current_candidate_code>\n{code_state}\n</current_candidate_code>
        What is your next spoken question?
        """

        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": MAX_TOKENS,
            "temperature": 0.7,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}]
        }

        logger.info("Calling Amazon Bedrock Claude 3.5 Sonnet...")
        response = bedrock_client.invoke_model(
            modelId=MODEL_ID,
            contentType='application/json',
            accept='application/json',
            body=json.dumps(payload)
        )
        
        result_body = json.loads(response.get('body').read())
        ai_question = result_body['content'][0]['text']
        
        logger.info("Question generated successfully.")
        
        # Return standard API/WebSocket format
        return {
            'statusCode': 200,
            'body': json.dumps({'ai_question': ai_question})
        }

    except ClientError as e:
        error_msg = e.response['Error']['Message']
        logger.error(f"Bedrock ClientError: {error_msg}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'AI Service Unavailable', 'details': error_msg})}
    except Exception as e:
        logger.error(f"Unknown System Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'Internal Server Error'})}