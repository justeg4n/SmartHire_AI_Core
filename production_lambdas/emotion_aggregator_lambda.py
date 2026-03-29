# =============================================================================
# emotion_aggregator_lambda.py — SmartHire AI | Emotion Data Aggregator
#
# Responsibility: Aggregate all per-frame emotion records for a session into
# final statistical metrics for the report.
#
# INVOCATION: Step Functions State 2 in the report pipeline, triggered at
# session end. Receives the session_id, queries DynamoDB for all EMOTION#
# records, and returns aggregated metrics.
#
# WHY QUERY DYNAMODB (instead of receiving emotion_timeline in the event):
#   The original code received the full emotion_timeline as an event payload.
#   At 5s intervals over a 30-minute session = 360 frames. Passing 360 JSON
#   objects through Step Functions state machine events approaches the 256KB
#   input/output limit and makes debugging harder. Querying DynamoDB directly
#   is the correct pattern — Step Functions passes only the session_id.
#
# DynamoDB QUERY:
#   PK = SESSION#<id>  SK begins_with EMOTION#
#   All records written by emotion_tracker_lambda.py during the session.
#
# RETURN (Step Functions output → State 3 Composite):
# {
#   "statusCode": 200,
#   "emotion_metrics": {
#     "total_frames":          360,
#     "percent_time_calm":     72.5,
#     "percent_time_stressed": 18.3,
#     "average_attention_score": 84.1,
#     "peak_stress_turn":      4,
#     "no_face_percent":       2.2,
#     "emotion_timeline_summary": [
#       { "turn": 1, "dominant": "CALM", "avg_attention": 91 }, ...
#     ]
#   }
# }
# =============================================================================

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cold-start singletons
# ---------------------------------------------------------------------------

_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
dynamodb = boto3.resource("dynamodb", region_name=_REGION)

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME", "SmartHire_ApplicationTracking")

# Confidence threshold for an emotion to be counted as 'stressed'
STRESS_CONFIDENCE_THRESHOLD = float(os.environ.get("STRESS_CONFIDENCE_THRESHOLD", "30.0"))

STRESS_EMOTIONS = {"FEAR", "CONFUSED", "SAD", "ANGRY", "DISGUSTED"}
CALM_EMOTIONS   = {"CALM", "HAPPY"}


# ---------------------------------------------------------------------------
# DynamoDB query
# ---------------------------------------------------------------------------

@xray_recorder.capture("QueryEmotionFrames")
def _query_frames(session_id: str) -> list:
    """
    Retrieve all EMOTION# records for the session from DynamoDB.

    Uses a KeyConditionExpression with begins_with to avoid a full-table scan.
    Paginates automatically via ExclusiveStartKey if there are > 1MB of records
    (unlikely for a single session but handled correctly).
    """
    table   = dynamodb.Table(DYNAMODB_TABLE)
    pk      = f"SESSION#{session_id}"
    frames  = []
    kwargs  = {
        "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
        "ExpressionAttributeValues": {":pk": pk, ":prefix": "EMOTION#"},
        "ScanIndexForward": True,
    }

    while True:
        resp  = table.query(**kwargs)
        frames.extend(resp.get("Items", []))
        last  = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last

    logger.info(f"Loaded {len(frames)} emotion frame(s) for session {session_id}.")
    return frames


# ---------------------------------------------------------------------------
# Aggregation logic
# ---------------------------------------------------------------------------

@xray_recorder.capture("AggregateEmotionMetrics")
def _aggregate(frames: list) -> dict:
    """
    Compute session-level emotion metrics from the per-frame records.

    New vs original:
    - Reads from DynamoDB (not from event payload)
    - Returns peak_stress_turn (which specific interview turn was hardest)
    - Returns emotion_timeline_summary per turn (useful for report chart)
    - Breaks down no-face percentage (quality indicator — was camera working?)
    - Uses the richer attentionScore already computed by emotion_tracker
      rather than re-computing from yaw/pitch here
    """
    total = len(frames)
    if total == 0:
        logger.warning("No emotion frames found.")
        return {
            "total_frames":             0,
            "percent_time_calm":        0.0,
            "percent_time_stressed":    0.0,
            "average_attention_score":  0.0,
            "peak_stress_turn":         None,
            "no_face_percent":          100.0,
            "emotion_timeline_summary": [],
        }

    calm_count     = 0
    stress_count   = 0
    no_face_count  = 0
    attention_sum  = 0

    # Per-turn accumulators for the timeline summary
    # turn_data[turn_number] = {"calm":0, "stress":0, "attention":[], "frames":0}
    turn_data: dict = defaultdict(lambda: {
        "calm": 0, "stress": 0, "attention": [], "frames": 0, "dominant_counts": defaultdict(int)
    })

    for frame in frames:
        dominant  = frame.get("dominantEmotion", "UNKNOWN")
        attention = int(frame.get("attentionScore", 0))
        turn      = int(frame.get("turnNumber", 0))
        emotions  = frame.get("emotionsProfile", [])

        turn_data[turn]["frames"]    += 1
        turn_data[turn]["attention"].append(attention)
        turn_data[turn]["dominant_counts"][dominant] += 1
        attention_sum += attention

        if dominant == "NO_FACE":
            no_face_count += 1
            continue

        if dominant in CALM_EMOTIONS:
            calm_count += 1
            turn_data[turn]["calm"] += 1

        # Check for any stress emotion above threshold in this frame
        is_stressed = any(
            e["type"] in STRESS_EMOTIONS and e["confidence"] >= STRESS_CONFIDENCE_THRESHOLD
            for e in emotions
        )
        if is_stressed:
            stress_count += 1
            turn_data[turn]["stress"] += 1

    # Identify peak stress turn
    peak_stress_turn = None
    max_stress_ratio = 0.0
    for turn_num, td in turn_data.items():
        if td["frames"] > 0:
            ratio = td["stress"] / td["frames"]
            if ratio > max_stress_ratio:
                max_stress_ratio = ratio
                peak_stress_turn = int(turn_num)

    # Build per-turn timeline summary
    timeline_summary = []
    for turn_num in sorted(turn_data.keys()):
        td = turn_data[turn_num]
        if td["frames"] == 0:
            continue
        dominant_name = max(td["dominant_counts"], key=td["dominant_counts"].get, default="UNKNOWN")
        timeline_summary.append({
            "turn":            int(turn_num),
            "frames":          td["frames"],
            "dominant":        dominant_name,
            "avg_attention":   round(sum(td["attention"]) / len(td["attention"]), 1) if td["attention"] else 0.0,
            "stress_percent":  round(td["stress"] / td["frames"] * 100, 1),
        })

    metrics = {
        "total_frames":             total,
        "percent_time_calm":        round(calm_count   / total * 100, 1),
        "percent_time_stressed":    round(stress_count / total * 100, 1),
        "average_attention_score":  round(attention_sum / total, 1),
        "peak_stress_turn":         peak_stress_turn,
        "no_face_percent":          round(no_face_count / total * 100, 1),
        "emotion_timeline_summary": timeline_summary,
    }

    logger.info(f"Aggregation complete: {json.dumps({k: v for k, v in metrics.items() if k != 'emotion_timeline_summary'})}")
    return metrics


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Step Functions State 2 handler.

    Reads session_id from the event (passed by Step Functions from State 1).
    Returns flat dict for Step Functions state output — no 'body' wrapping.
    """
    session_id = event.get("session_id", "unknown")
    logger.info(f"EmotionAggregator: session={session_id}")

    try:
        frames  = _query_frames(session_id)
        metrics = _aggregate(frames)

        return {
            "statusCode":     200,
            "session_id":     session_id,
            "emotion_metrics": metrics,
        }

    except Exception as exc:
        logger.error(f"EmotionAggregator failed: {exc}", exc_info=True)
        raise  # Raise so Step Functions retries
