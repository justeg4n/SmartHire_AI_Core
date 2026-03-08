import json
import logging
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@xray_recorder.capture('AggregateEmotionMetrics')
def lambda_handler(event, context):
    """
    Step Functions State 2: Aggregates timeline data into final metrics.
    Expected event payload: {"emotion_timeline": [ {frame1}, {frame2}, ... ]}
    """
    logger.info("Initializing Emotion Aggregator Lambda")
    
    try:
        emotion_timeline = event.get('emotion_timeline', [])
        total_frames = len(emotion_timeline)
        
        if total_frames == 0:
            logger.warning("No emotion data provided.")
            return {
                'statusCode': 200,
                'emotion_metrics': {
                    "percent_time_calm": 0,
                    "average_attention_score": 0,
                    "overall_stress_level": 0
                }
            }

        calm_frames = 0
        stress_frames = 0
        good_attention_frames = 0
        
        STRESS_EMOTIONS = ["FEAR", "CONFUSED", "SAD", "ANGRY"]
        STRESS_THRESHOLD = 30.0
        YAW_TOLERANCE = 25.0
        PITCH_TOLERANCE = 20.0

        for frame in emotion_timeline:
            # 1. Attention Calculation
            pose = frame.get('head_pose', {'yaw': 0, 'pitch': 0})
            if abs(pose.get('yaw', 0)) <= YAW_TOLERANCE and abs(pose.get('pitch', 0)) <= PITCH_TOLERANCE:
                good_attention_frames += 1

            # 2. Emotion Calculation
            emotions = frame.get('emotions_profile', [])
            if not emotions:
                continue
                
            top_emotion = emotions[0]
            
            if top_emotion['type'] in ["CALM", "HAPPY"]:
                calm_frames += 1
                
            for emotion in emotions:
                if emotion['type'] in STRESS_EMOTIONS and emotion['confidence'] >= STRESS_THRESHOLD:
                    stress_frames += 1
                    break

        # Calculate final percentages
        metrics = {
            "percent_time_calm": round((calm_frames / total_frames) * 100, 1),
            "average_attention_score": round((good_attention_frames / total_frames) * 100, 1),
            "overall_stress_level": round((stress_frames / total_frames) * 100, 1)
        }
        
        logger.info(f"Aggregation complete: {json.dumps(metrics)}")

        return {
            'statusCode': 200,
            'emotion_metrics': metrics
        }

    except Exception as e:
        logger.error(f"Error during aggregation: {str(e)}")
        raise e