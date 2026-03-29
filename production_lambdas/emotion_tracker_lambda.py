# =============================================================================
# emotion_tracker_lambda.py — SmartHire AI | Per-Frame Emotion Analysis
#
# Responsibility: Analyse a single webcam frame for facial emotion, attention
# (head pose), and eye contact proxy. Write the result to DynamoDB and return
# it in Step-Functions-compatible format.
#
# INVOCATION: Two modes.
#   A) Direct async invoke from .NET — every 5 seconds during the live session.
#      .NET sends the canvas frame as base64 + session context.
#   B) Step Functions Map state (batch) — at session end, the report workflow
#      may re-process stored frames. Payload format is identical.
#
# WHY DynamoDB WRITE HERE (not just return):
#   The original code only returned frame data. The report pipeline later has
#   no data to aggregate because nothing was persisted. Every frame result
#   must be written immediately so it survives independent of whether the
#   Lambda that called us stays alive.
#
# DynamoDB RECORD WRITTEN:
#   PK = SESSION#<session_id>  SK = EMOTION#<timestamp_ms>
#   {
#     sessionId, turnNumber, timestamp,
#     emotionsProfile: [{ type, confidence }],
#     headPose: { yaw, pitch, roll },
#     attentionScore: 0-100,
#     dominantEmotion: "CALM"
#   }
#
# INPUT EVENT SCHEMA:
# {
#   "session_id":    "uuid",
#   "turn_number":   5,        # which interview turn this frame belongs to
#   "image_base64":  "...",    # JPEG or PNG from Canvas.toDataURL()
# }
# =============================================================================

import base64
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from aws_xray_sdk.core import patch_all, xray_recorder

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cold-start singletons
# ---------------------------------------------------------------------------

_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")

rekognition_client = boto3.client("rekognition", region_name=_REGION)
dynamodb           = boto3.resource("dynamodb",   region_name=_REGION)

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME", "SmartHire_ApplicationTracking")

# Head-pose thresholds for attention scoring.
# Yaw (horizontal) > 25° = candidate looking significantly off-camera.
# Pitch (vertical) > 20° = candidate looking at the floor or ceiling.
# These are conservative values — minor adjustment is normal and ignored.
YAW_TOLERANCE   = float(os.environ.get("ATTENTION_YAW_TOLERANCE",   "25.0"))
PITCH_TOLERANCE = float(os.environ.get("ATTENTION_PITCH_TOLERANCE", "20.0"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ms() -> int:
    """Millisecond timestamp — used as the DynamoDB SK suffix for uniqueness."""
    import time
    return int(time.time() * 1000)


def _compute_attention_score(yaw: float, pitch: float) -> int:
    """
    Return an attention score 0-100 based on head pose angles.

    Formula: start at 100, subtract penalties proportional to how far outside
    the tolerance zone the candidate's head has rotated.
    - Full yaw tolerance = 0 penalty; double the tolerance = 40 penalty max.
    - Full pitch tolerance = 0 penalty; double the tolerance = 30 penalty max.
    Clamp to [0, 100].

    WHY HEAD POSE AS PROXY: Rekognition does not provide gaze direction
    (eye tracking requires specialised hardware). Head pose is a reliable
    proxy — candidates who look away from the screen rotate their head,
    not just their eyes.
    """
    abs_yaw   = abs(yaw)
    abs_pitch = abs(pitch)
    penalty   = 0

    if abs_yaw > YAW_TOLERANCE:
        yaw_excess = abs_yaw - YAW_TOLERANCE
        penalty   += min(40, int(yaw_excess / YAW_TOLERANCE * 40))

    if abs_pitch > PITCH_TOLERANCE:
        pitch_excess = abs_pitch - PITCH_TOLERANCE
        penalty     += min(30, int(pitch_excess / PITCH_TOLERANCE * 30))

    return max(0, 100 - penalty)


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

@xray_recorder.capture("AnalyseEmotionFrame")
def _analyse_frame(image_bytes: bytes) -> dict:
    """
    Call Rekognition DetectFaces and extract the primary face's
    emotion profile and head pose.

    We always take FaceDetails[0] — the highest-confidence face — because
    interview sessions should have exactly one face (the candidate).
    If no face is detected, we return a neutral placeholder so the
    aggregator has consistent data structure across all frames.

    Emotions are returned sorted by confidence descending. The first element
    is the 'dominant emotion' for that frame.
    """
    resp = rekognition_client.detect_faces(
        Image={"Bytes": image_bytes},
        Attributes=["ALL"],
    )

    faces = resp.get("FaceDetails", [])
    if not faces:
        logger.warning("No face detected in frame.")
        return {
            "emotionsProfile": [],
            "headPose":        {"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
            "attentionScore":  0,
            "dominantEmotion": "NO_FACE",
        }

    face = faces[0]

    # Sort all 8 emotion types by confidence descending
    sorted_emotions = sorted(
        face.get("Emotions", []),
        key=lambda e: e["Confidence"],
        reverse=True,
    )
    emotions_profile = [
        {"type": e["Type"], "confidence": round(e["Confidence"], 2)}
        for e in sorted_emotions
    ]

    pose = face.get("Pose", {})
    yaw   = round(pose.get("Yaw",   0.0), 2)
    pitch = round(pose.get("Pitch", 0.0), 2)
    roll  = round(pose.get("Roll",  0.0), 2)

    attention = _compute_attention_score(yaw, pitch)
    dominant  = emotions_profile[0]["type"] if emotions_profile else "UNKNOWN"

    return {
        "emotionsProfile": emotions_profile,
        "headPose":        {"yaw": yaw, "pitch": pitch, "roll": roll},
        "attentionScore":  attention,
        "dominantEmotion": dominant,
    }


# ---------------------------------------------------------------------------
# DynamoDB persistence
# ---------------------------------------------------------------------------

def _save_frame(session_id: str, turn_number: int, frame_data: dict) -> str:
    """
    Write frame analysis result to DynamoDB.

    SK uses millisecond timestamp to guarantee uniqueness even if multiple
    frames arrive within the same second. This also gives natural chronological
    ordering within the session partition.

    The emotion_aggregator queries all EMOTION# records for a session with:
    KeyConditionExpression = "PK = :pk AND begins_with(SK, :prefix)"
    where prefix = "EMOTION#"
    """
    table    = dynamodb.Table(DYNAMODB_TABLE)
    ts_ms    = _now_ms()
    ts_iso   = _now_iso()

    item = {
        "PK":             f"SESSION#{session_id}",
        "SK":             f"EMOTION#{ts_ms}",
        "type":           "INTERVIEW_EMOTION_FRAME",
        "sessionId":      session_id,
        "turnNumber":     turn_number,
        "timestamp":      ts_iso,
        **frame_data,
    }

    table.put_item(Item=item)
    sk = f"EMOTION#{ts_ms}"
    logger.info(f"Emotion frame saved: {sk}  dominant={frame_data.get('dominantEmotion')}  "
                f"attention={frame_data.get('attentionScore')}")
    return sk


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

@xray_recorder.capture("EmotionTracker")
def lambda_handler(event, context):
    """
    Analyse one webcam frame, persist it, and return the result.

    RETURN FORMAT: flat dict (NOT wrapped in 'body' JSON string).
    Step Functions passes the return value directly as the next state's input.
    Wrapping in {'body': json.dumps(...)} would require every downstream state
    to call json.loads — error-prone and unnecessary.
    """
    session_id   = event.get("session_id",  "unknown")
    turn_number  = int(event.get("turn_number", 0))
    image_b64    = event.get("image_base64")

    logger.info(f"EmotionTracker: session={session_id} turn={turn_number}")

    if not image_b64:
        logger.warning("No image_base64 in event.")
        return {
            "statusCode":    400,
            "session_id":    session_id,
            "error":         "No image_base64 provided",
            "emotionsProfile": [],
            "headPose":        {"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
            "attentionScore":  0,
            "dominantEmotion": "NO_IMAGE",
        }

    try:
        image_bytes = base64.b64decode(image_b64)
        frame_data  = _analyse_frame(image_bytes)
        saved_sk    = _save_frame(session_id, turn_number, frame_data)

        return {
            "statusCode":   200,
            "session_id":   session_id,
            "turn_number":  turn_number,
            "saved_sk":     saved_sk,
            **frame_data,
        }

    except ClientError as e:
        error_msg = e.response["Error"]["Message"]
        logger.error(f"Rekognition error: {error_msg}")
        return {
            "statusCode":    500,
            "session_id":    session_id,
            "error":         f"Rekognition error: {error_msg}",
            "emotionsProfile": [],
            "headPose":        {"yaw": 0.0, "pitch": 0.0, "roll": 0.0},
            "attentionScore":  0,
            "dominantEmotion": "ERROR",
        }

    except Exception as exc:
        logger.error(f"EmotionTracker failed: {exc}", exc_info=True)
        # Raise so Step Functions can retry via its built-in retry policy
        raise
