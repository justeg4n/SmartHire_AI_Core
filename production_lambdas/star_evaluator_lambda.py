# =============================================================================
# star_evaluator_lambda.py — SmartHire AI | STAR Answer Evaluator
#
# Responsibility: Evaluate every Q&A turn in the session using the STAR
# method and return per-question scores + composite summary.
#
# INVOCATION: Step Functions State 1 (Parallel Map) in the report pipeline.
# Step Functions runs one invocation per turn in parallel. Each invocation
# receives a single turn's question and answer, evaluates it, and returns
# the score. Step Functions collects all results automatically.
#
# WHY PARALLEL MAP (not a loop in one Lambda):
#   A 10-turn session = 10 independent Bedrock calls. Running them sequentially
#   in a single Lambda would take ~20 seconds (2s each). Step Functions Map
#   state runs all 10 concurrently — total wall time ≈ 2-3s for the full batch.
#   This is one of the key advantages of Step Functions for AI workflows.
#
# WHY QUERY DYNAMODB HERE (not receive turns in event):
#   Step Functions can pass all turns as an array input but at scale (10+
#   turns × potentially large code snapshots) this approaches the 256KB event
#   limit. Querying DynamoDB for individual turns by SK is cleaner and safe.
#
# INPUT EVENT (one per turn from Step Functions Map):
# {
#   "session_id":   "uuid",
#   "turn_number":  3,       # Step Functions iterator.index
#   "sk":           "TURN#0003"  # optional direct SK
# }
#
# RETURN (flat dict, Step Functions output for this Map item):
# {
#   "statusCode":    200,
#   "session_id":    "uuid",
#   "turn_number":   3,
#   "question":      "...",
#   "answer":        "...",
#   "star_evaluation": {
#     "technical_accuracy":   8,
#     "communication_clarity": 7,
#     "star_structure":        6,
#     "depth_of_reasoning":    7,
#     "composite_score":       72.5,
#     "justification":         "..."
#   }
# }
# =============================================================================

import json
import logging
import os
import re
from decimal import Decimal

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
_retry  = Config(region_name=_REGION, retries={"max_attempts": 3, "mode": "adaptive"})

bedrock_client = boto3.client("bedrock-runtime", config=_retry)
dynamodb       = boto3.resource("dynamodb", region_name=_REGION)

MODEL_ID       = os.environ.get("BEDROCK_MODEL_ID", "apac.anthropic.claude-3-5-sonnet-20241022-v2:0")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE_NAME", "SmartHire_ApplicationTracking")

# Scoring weights for the composite score (must sum to 1.0)
WEIGHT_TECHNICAL  = float(os.environ.get("SCORE_WEIGHT_TECHNICAL",   "0.40"))
WEIGHT_COMM       = float(os.environ.get("SCORE_WEIGHT_COMM",        "0.30"))
WEIGHT_STAR       = float(os.environ.get("SCORE_WEIGHT_STAR",        "0.15"))
WEIGHT_DEPTH      = float(os.environ.get("SCORE_WEIGHT_DEPTH",       "0.15"))


# ---------------------------------------------------------------------------
# DynamoDB fetch
# ---------------------------------------------------------------------------

@xray_recorder.capture("FetchTurn")
def _fetch_turn(session_id: str, turn_number: int) -> dict:
    """
    Fetch a single turn record from DynamoDB.

    SK = TURN#<zero_padded_4_digit> written by ai_orchestrator_lambda.
    Returns the DynamoDB Item or raises ValueError if not found.
    """
    table = dynamodb.Table(DYNAMODB_TABLE)
    sk    = f"TURN#{turn_number:04d}"
    resp  = table.get_item(
        Key={"PK": f"SESSION#{session_id}", "SK": sk}
    )
    item = resp.get("Item")
    if not item:
        raise ValueError(f"Turn {sk} not found for session {session_id}.")
    return item


# ---------------------------------------------------------------------------
# Safe JSON extraction from Bedrock output
# ---------------------------------------------------------------------------

def _parse_bedrock_json(raw_text: str) -> dict | None:
    """
    Safely extract a JSON object from Bedrock's response text.

    WHY THIS IS NECESSARY: Even with temperature=0.1 and an explicit
    "output ONLY raw JSON" instruction, Bedrock occasionally wraps output in
    markdown code fences (```json ... ```) or adds a preamble sentence.
    The original code called json.loads() directly — a single malformed
    response would crash the Step Functions branch for that turn, failing
    the entire report. We handle three common output formats defensively.
    """
    text = raw_text.strip()

    # Format 1: clean JSON directly
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Format 2: wrapped in ```json ... ```
    fence_match = re.search(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", text, re.IGNORECASE)
    if fence_match:
        try:
            return json.loads(fence_match.group(1))
        except json.JSONDecodeError:
            pass

    # Format 3: JSON object embedded in prose — extract the first { ... } block
    obj_match = re.search(r"\{[\s\S]+\}", text)
    if obj_match:
        try:
            return json.loads(obj_match.group(0))
        except json.JSONDecodeError:
            pass

    logger.error(f"Failed to extract JSON from Bedrock output: '{text[:200]}'")
    return None


# ---------------------------------------------------------------------------
# Bedrock STAR evaluation
# ---------------------------------------------------------------------------

@xray_recorder.capture("EvaluateSTAR")
def _evaluate(question: str, answer: str, code_snapshot: str) -> dict:
    """
    Evaluate the candidate's answer using the STAR rubric via Bedrock.

    WHY FOUR DIMENSIONS (not three like the original):
    - technical_accuracy:   Does the answer demonstrate real knowledge?
    - communication_clarity: Was the response structured and clear?
    - star_structure:        Did the answer follow Situation/Task/Action/Result?
    - depth_of_reasoning:    Did the candidate explain WHY, not just WHAT?
    Adding depth_of_reasoning catches shallow memorised answers that score
    well on STAR structure but show no real understanding.

    Code context is included if non-empty so the evaluator can assess whether
    the code logic matches the verbal explanation.
    """
    code_section = ""
    if code_snapshot and code_snapshot.strip():
        code_section = f"\n<candidate_code>\n{code_snapshot.strip()[:1000]}\n</candidate_code>"

    system_prompt = """You are an Expert Technical Interview Grader for SmartHire.
Evaluate the candidate's answer using the STAR method and technical accuracy.

Output ONLY a raw JSON object. No markdown, no explanation, no preamble.

Required schema:
{
  "technical_accuracy":    <integer 0-10>,
  "communication_clarity": <integer 0-10>,
  "star_structure":        <integer 0-10>,
  "depth_of_reasoning":    <integer 0-10>,
  "justification":         "<2-sentence factual explanation of the scores>"
}"""

    user_message = (
        f"<interview_question>\n{question}\n</interview_question>\n"
        f"<candidate_answer>\n{answer}\n</candidate_answer>"
        f"{code_section}\n\nPlease grade this response."
    )

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens":        300,
        "temperature":       0.1,
        "system":            system_prompt,
        "messages":          [{"role": "user", "content": user_message}],
    }

    resp   = bedrock_client.invoke_model(
        modelId=MODEL_ID, contentType="application/json",
        accept="application/json", body=json.dumps(payload),
    )
    body   = json.loads(resp["body"].read())
    raw    = body["content"][0]["text"]
    result = _parse_bedrock_json(raw)

    if result is None:
        # Return a neutral score with an error flag rather than crashing
        logger.error("Using fallback scores because Bedrock output was unparseable.")
        return {
            "technical_accuracy":   5,
            "communication_clarity": 5,
            "star_structure":        5,
            "depth_of_reasoning":    5,
            "justification":         "Evaluation data was unavailable for this turn.",
            "parse_error":           True,
        }

    # Clamp all scores to [0, 10]
    for field in ["technical_accuracy", "communication_clarity", "star_structure", "depth_of_reasoning"]:
        result[field] = max(0, min(10, int(result.get(field, 5))))

    # Compute weighted composite (0-100 scale)
    composite = (
        result["technical_accuracy"]    * WEIGHT_TECHNICAL +
        result["communication_clarity"] * WEIGHT_COMM +
        result["star_structure"]        * WEIGHT_STAR +
        result["depth_of_reasoning"]    * WEIGHT_DEPTH
    ) * 10

    result["composite_score"] = round(composite, 1)
    return result


# ---------------------------------------------------------------------------
# Lambda Entry Point
# ---------------------------------------------------------------------------

@xray_recorder.capture("STAREvaluator")
def lambda_handler(event, context):
    """
    Evaluate one interview turn.

    Step Functions Map state calls this once per turn in parallel.
    Returns flat dict — Step Functions collects all parallel results
    into an array automatically.
    """
    session_id  = event.get("session_id",  "unknown")
    turn_number = int(event.get("turn_number", 0))

    logger.info(f"STAREvaluator: session={session_id} turn={turn_number}")

    try:
        # Fetch turn from DynamoDB
        turn_item = _fetch_turn(session_id, turn_number)

        question      = str(turn_item.get("aiResponse",         "")).strip()
        answer        = str(turn_item.get("candidateTranscript", "")).strip()
        code_snapshot = str(turn_item.get("codeStateSnapshot",  "")).strip()

        if not question or not answer:
            logger.warning(f"Turn {turn_number} has empty question or answer — skipping evaluation.")
            return {
                "statusCode":   200,
                "session_id":   session_id,
                "turn_number":  turn_number,
                "question":     question,
                "answer":       answer,
                "star_evaluation": {
                    "technical_accuracy":    0,
                    "communication_clarity": 0,
                    "star_structure":        0,
                    "depth_of_reasoning":    0,
                    "composite_score":       0.0,
                    "justification":         "No response recorded for this turn.",
                },
            }

        evaluation = _evaluate(question, answer, code_snapshot)
        logger.info(f"Turn {turn_number} composite score: {evaluation.get('composite_score')}")

        return {
            "statusCode":    200,
            "session_id":    session_id,
            "turn_number":   turn_number,
            "question":      question,
            "answer":        answer,
            "star_evaluation": evaluation,
        }

    except ValueError as exc:
        logger.error(f"Turn not found: {exc}")
        # Return a graceful placeholder rather than crashing the Map state
        return {
            "statusCode":   404,
            "session_id":   session_id,
            "turn_number":  turn_number,
            "error":        str(exc),
            "star_evaluation": {
                "technical_accuracy": 0, "communication_clarity": 0,
                "star_structure": 0, "depth_of_reasoning": 0,
                "composite_score": 0.0, "justification": "Turn data not found.",
            },
        }

    except ClientError as exc:
        logger.error(f"Bedrock ClientError: {exc}")
        raise  # Let Step Functions retry

    except Exception as exc:
        logger.error(f"STAREvaluator failed: {exc}", exc_info=True)
        raise
