import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock_client = boto3.client('bedrock-runtime')
MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')

@xray_recorder.capture('GenerateExecutiveSummary')
def lambda_handler(event, context):
    """
    AWS Lambda Handler for AWS Step Functions (State 4: Narrative).
    Receives aggregated data from State 3 (Composite).
    """
    logger.info(f"Starting Executive Report Generation. Event input: {json.dumps(event)}")
    
    try:
        star_scores = event.get('star_scores', {})
        emotion_metrics = event.get('emotion_metrics', {})
        code_results = event.get('code_results', {})
        
        system_prompt = """You are an Expert Technical Hiring Manager for SmartHire.
        Write a 300-word executive summary evaluating the candidate based on the provided JSON data metrics.
        Highlight technical strengths, communication skills, and behavior under stress.
        Conclude with a definitive HIRE or NO HIRE recommendation based strictly on the data.
        Output ONLY the narrative text."""

        user_message = f"""
        <star_evaluation_scores>\n{json.dumps(star_scores)}\n</star_evaluation_scores>
        <emotion_and_attention_metrics>\n{json.dumps(emotion_metrics)}\n</emotion_and_attention_metrics>
        <coding_sandbox_results>\n{json.dumps(code_results)}\n</coding_sandbox_results>
        Please write the executive summary.
        """

        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 500,
            "temperature": 0.2, # Keep low for an objective, factual report
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}]
        }

        response = bedrock_client.invoke_model(
            modelId=MODEL_ID,
            contentType='application/json',
            accept='application/json',
            body=json.dumps(payload)
        )
        
        result_body = json.loads(response.get('body').read())
        executive_summary = result_body['content'][0]['text']
        
        logger.info("Report generated successfully.")
        
        # Return payload for State 5 of Step Functions (Persist to S3/RDS)
        return {
            'statusCode': 200,
            'report_summary': executive_summary,
            'raw_metrics_used': event # Pass the raw data down to the next state if needed
        }

    except ClientError as e:
        logger.error(f"Bedrock Error: {e.response['Error']['Message']}")
        raise e # In Step Functions, we usually raise the error so Step Functions can auto-retry
    except Exception as e:
        logger.error(f"Unexpected Error: {str(e)}")
        raise e