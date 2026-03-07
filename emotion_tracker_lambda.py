import json
import boto3
import base64
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

rekognition_client = boto3.client('rekognition')

def lambda_handler(event, context):
    logger.info("Initializing Emotion Tracker Lambda")
    
    try:
        # Backend will pass the webcam frame as a Base64 string
        base64_image = event.get('image_base64')
        if not base64_image:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No image_base64 provided'})}
            
        # Decode the image back into binary bytes for AWS Rekognition
        image_bytes = base64.b64decode(base64_image)
        
        logger.info("Calling Amazon Rekognition...")
        response = rekognition_client.detect_faces(
            Image={'Bytes': image_bytes},
            Attributes=['ALL']
        )
        
        if not response['FaceDetails']:
            return {'statusCode': 200, 'body': json.dumps({'warning': 'No face detected'})}
            
        candidate_face = response['FaceDetails'][0]
        
        # Extract ALL 8 Emotions
        sorted_emotions = sorted(candidate_face['Emotions'], key=lambda x: x['Confidence'], reverse=True)
        formatted_emotions = [{"type": e['Type'], "confidence": round(e['Confidence'], 2)} for e in sorted_emotions]
        
        # Extract Head Pose
        pose = candidate_face['Pose']
        
        result = {
            "emotions_profile": formatted_emotions,
            "head_pose": {"yaw": round(pose['Yaw'], 2), "pitch": round(pose['Pitch'], 2)}
        }
        
        return {'statusCode': 200, 'body': json.dumps(result)}

    except Exception as e:
        logger.error(f"Error processing frame: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}