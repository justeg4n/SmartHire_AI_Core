import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cold Start Optimization
bedrock_client = boto3.client('bedrock-runtime')
MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')

@xray_recorder.capture('EvaluateSTARMethod')
def lambda_handler(event, context):
    """
    Step Functions State 1: Evaluates candidate answers using the STAR rubric.
    Expected event payload: {"question": "...", "answer": "..."}
    """
    logger.info("Initializing STAR Evaluator Lambda")
    
    try:
        question = event.get('question', '')
        answer = event.get('answer', '')
        
        if not question or not answer:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Missing question or answer in payload'})}

        system_prompt = """You are an Expert Technical Interview Grader.
        Evaluate the candidate's answer to the question using the STAR method (Situation, Task, Action, Result).
        
        You MUST output ONLY a raw JSON object with no markdown formatting.
        Schema:
        {
            "technical_accuracy": <int 0-10>,
            "communication_clarity": <int 0-10>,
            "star_structure": <int 0-10>,
            "justification": "<brief 2-sentence explanation>"
        }"""

        user_message = f"""
        <interview_question>\n{question}\n</interview_question>
        <candidate_answer>\n{answer}\n</candidate_answer>
        Please grade this response.
        """

        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 300,
            "temperature": 0.1, # Low temperature for consistent grading
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}]
        }

        logger.info("Calling Amazon Bedrock for evaluation...")
        response = bedrock_client.invoke_model(
            modelId=MODEL_ID,
            contentType='application/json',
            accept='application/json',
            body=json.dumps(payload)
        )
        
        result_body = json.loads(response.get('body').read())
        evaluation_json_string = result_body['content'][0]['text']
        
        # Parse the stringified JSON from Bedrock into an actual Python dictionary
        evaluation_data = json.loads(evaluation_json_string)
        logger.info("Evaluation completed successfully.")
        
        # Step Functions requires a clean return dictionary to pass to the next state
        return {
            'statusCode': 200,
            'star_evaluation': evaluation_data
        }

    except ClientError as e:
        logger.error(f"Bedrock Error: {e.response['Error']['Message']}")
        raise e # Let Step Functions handle the retry logic
    except Exception as e:
        logger.error(f"System Error: {str(e)}")
        raise e