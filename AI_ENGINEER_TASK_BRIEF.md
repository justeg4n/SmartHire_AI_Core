# AI Engineer — Technical Task Brief
## SmartHire: Bidirectional Matching System (Flow 1 + Flow 3)

**Document type:** Engineering task specification  
**Owner:** AI Engineer  
**Status:** Ready to implement  
**Depends on:** `vector_ops.py`, `vector_persister.py`, `text_processor.py`, `ingestion_trigger.py` (all complete ✅)

---

## 1. Context: What Is Already Done vs What You Must Build

### ✅ Completed (Flow 2 — Per-JD Application)

```
Candidate uploads CV to a specific Job
    → S3 Event → SQS → ingestion_trigger.py
    → Step Functions (TextractTask → ClaudeTask → VectorOpsTask → PersisterTask)
    → text_processor.py  : Textract + spaCy PII masking + Claude Agent 1 (skills) + Claude Agent 2 (interview questions)
    → vector_ops.py      : Cohere Bi-Encoder + Cross-Encoder rerank + Hybrid score vs THIS ONE JD
    → vector_persister.py: pgvector upsert + DynamoDB write
```

Result stored in DynamoDB:
```
PK = CANDIDATE#{candidateId}
SK = CV#{fileKey}
Fields: parsedResult, scoringDetails (bi+cross+hybrid), interviewGuide, maskingReport
```

---

### ❌ NOT YET BUILT — Your Tasks

**Flow 1 — Global CV Profile Upload (Candidate-initiated, no JD context)**

```
Candidate uploads CV to their profile page (no jobId)
    → S3 key: candidates/{candidateId}/cv.pdf   ← no jobId segment
    → SQS → ingestion_trigger.py detects job_id = None
    → Step Functions (PROFILE workflow) → text_processor → vector_ops (embed only, no scoring)
    → vector_persister writes vector to pgvector
    → [NEW] job_suggestion_engine.py
        ├── pgvector kNN: CV vector vs all job_embeddings → top 20 candidates
        ├── Cross-Encoder rerank: top 20 → top 5 precise matches
        ├── Claude Agent 3: generate "why this job fits you" narrative per match
        └── DynamoDB write: CANDIDATE#{id} / JOB_SUGGESTIONS
```

**Flow 3 — JD Posted → Ranked Candidate List (Recruiter-initiated)**

```
Recruiter creates a new Job
    → jd_parser.py already embeds JD vector → stored in DynamoDB + pgvector ✅
    → [NEW] SNS event "JOB_PUBLISHED" → SQS → candidate_ranking_engine.py
        ├── pgvector kNN: JD vector vs all candidate_embeddings → top 50
        ├── Cross-Encoder rerank: top 50 → top 10 ranked by relevance
        ├── Claude Agent 4: generate per-candidate "recruiter summary" narrative
        └── DynamoDB write: JOB#{jobId} / RANKED_CANDIDATES
```

---

## 2. Gap Analysis in Existing Files

Before building new files, patch these specific issues:

### `ingestion_trigger.py` — Patch Required

**File:** `ingestion_trigger.py`  
**Function:** `extract_job_id_and_candidate_from_key()`  
**Problem:** When path is `candidates/{candidateId}/cv.pdf`, `job_id` is correctly returned as `None`. But `start_processing()` always sends the same payload shape. The Step Functions execution input doesn't flag `flow_mode`.  
**Fix:** In `start_processing()`, add `flow_mode` to the payload:

```python
def start_processing(payload: dict) -> dict:
    enriched = {
        **payload,
        # "PROFILE_UPLOAD" when job_id is None → triggers global suggestion engine.
        # "JD_APPLICATION" when job_id is present → existing per-JD scoring flow.
        "flow_mode": "PROFILE_UPLOAD" if not payload.get("job_id") else "JD_APPLICATION",
    }

    if STATE_MACHINE_ARN:
        # Route to different state machines based on flow
        arn = os.environ.get(
            "PROFILE_STATE_MACHINE_ARN" if not payload.get("job_id") else "STATE_MACHINE_ARN",
            STATE_MACHINE_ARN
        )
        response = sfn.start_execution(stateMachineArn=arn, input=json.dumps(enriched))
        return {"started": True, "execution_arn": response.get("executionArn")}

    return invoke_vector_ops(enriched)
```

Add env var: `PROFILE_STATE_MACHINE_ARN` (Step Functions ARN for the new global profile workflow).

---

### `vector_ops.py` — Patch Required

**File:** `vector_ops.py`  
**Function:** `lambda_handler()`  
**Problem:** Lines 956–959. When `jd_text` is absent/default, scoring is correctly skipped, but the function just returns the cv_vector without triggering the suggestion engine. There is no handoff to `job_suggestion_engine`.  
**Fix:** After PHASE 2 completes (cv_vector computed), add a routing step:

```python
# At end of PHASE 2 (after cv_vector is computed, before the return statement)
flow_mode = event.get("flow_mode", "JD_APPLICATION")

if flow_mode == "PROFILE_UPLOAD":
    # Global profile mode: no per-JD scoring was done.
    # Signal the Step Functions orchestrator to invoke job_suggestion_engine next.
    # (In Step Functions, this return value feeds into the next Choice state.)
    return {
        **current_return_dict,
        "flow_mode": "PROFILE_UPLOAD",
        "trigger_suggestion_engine": True,
    }
```

In Step Functions, add a `Choice` state after `VectorOpsTask`:
- `$.flow_mode == "PROFILE_UPLOAD"` → next state is `JobSuggestionTask`
- else → next state is `VectorPersisterTask` (existing path)

---

### `vector_persister.py` — Patch Required

**File:** `vector_persister.py`  
**Function:** `save_to_dynamodb()`  
**Problem:** `topMatches` is stored as `to_decimal(top_matches)` but `top_matches` is a list of dicts, not a numeric dict. `to_decimal()` works because `json.loads/dumps` is used, but the field name and structure are inconsistent with what `job_suggestion_engine` will write. Align to a shared schema.  
**Fix:** Replace `"topMatches"` with `"jobSuggestions"` and ensure the schema matches (see Section 4).

---

## 3. New Files to Build

### File 1: `job_suggestion_engine.py`

**Lambda name:** `smarthire-job-suggestion-engine`  
**Trigger:** Step Functions task state (invoked after `VectorOpsTask` in the PROFILE_UPLOAD flow)  
**Input payload** (from vector_ops output):

```json
{
  "profile_id": "uuid-candidate",
  "file_key": "candidates/uuid/cv.pdf",
  "cv_vector": [0.12, 0.45, ...],
  "parsed_data": { "seniority_estimate": "Mid", "backend_skills": [...], ... },
  "masked_cv_text": "Experienced software engineer with 4 years...",
  "masking_report": { "blind_screening": "applied", ... },
  "flow_mode": "PROFILE_UPLOAD"
}
```

**Output payload** (returned to Step Functions → feeds VectorPersisterTask):

```json
{
  "profile_id": "uuid-candidate",
  "file_key": "candidates/uuid/cv.pdf",
  "job_suggestions": [
    {
      "jobId": "job-uuid",
      "jobTitle": "Senior Backend Engineer",
      "company": "Acme Corp",
      "bi_score": 78.4,
      "cross_encoder_score": 82.1,
      "final_score": 80.7,
      "fit_narrative": "Your 4 years of .NET experience and AWS Lambda work directly match the core stack. The main gap is Kubernetes — this company uses EKS heavily.",
      "rank": 1
    }
  ]
}
```

**Implementation steps:**

```
Step 1 — pgvector kNN Search (top 20 candidates)
    Query: SELECT job_id, title, company, (embedding <-> cv_vector) AS dist
            FROM job_embeddings
            JOIN Jobs ON job_id = Jobs.Id
            WHERE Jobs.is_active = true
            ORDER BY dist ASC
            LIMIT 20;
    
    Note: Use L2 distance (<->) not inner product (<#>) because Cohere embed-v3
    vectors are NOT guaranteed to be unit-norm. L2 is safe for all cases.

Step 2 — Cross-Encoder Reranking (top 20 → top 5)
    For each of the 20 jobs fetched, retrieve the jd_text from DynamoDB 
    (or pgvector job_embeddings.raw_text).
    
    Build pairs: [(jd_text[:1500], cv_chunk) for each cv_chunk]
    Run cross-encoder/ms-marco-MiniLM-L-6-v2 (reuse _get_cross_encoder() pattern)
    Aggregate chunk scores with weighted top-K average (existing pattern in vector_ops.py)
    
    Re-sort by cross_encoder_score descending → top 5 final matches.

Step 3 — Claude Agent 3: Fit Narrative Generation (parallel for top 5)
    For each of the 5 final matches, invoke Bedrock with a new prompt:
    
    System: "You are a Career Advisor AI. Given a candidate profile and a job description,
             write a 2-sentence explanation of why this job is a good fit for them,
             and one specific skill gap they should highlight in an application.
             Output JSON: { 'fit_narrative': '...', 'key_gap': '...' }"
    
    User:   "Candidate skills: {parsed_data skills summary}
             Job: {job_title} at {company}
             Job requirements summary: {jd_text[:800]}
             Match score: {final_score}%"
    
    Use asyncio or concurrent.futures.ThreadPoolExecutor(max_workers=5) to run
    all 5 Bedrock calls in parallel — NOT sequential. This keeps total latency
    under 3 seconds instead of 15.

Step 4 — Assemble and return full suggestion list
    Return the structured list to Step Functions.
    VectorPersisterTask writes it to DynamoDB.
```

**DynamoDB write schema** (handled by vector_persister, but YOU define the structure):

```
PK = CANDIDATE#{candidateId}
SK = JOB_SUGGESTIONS    ← fixed SK, not per-file
GSI1PK = "JOB_SUGGESTIONS"
GSI1SK = updatedAt      ← enables "latest suggestions" query
TTL = 7 days            ← suggestions refresh when CV is re-uploaded

Fields:
  jobSuggestions: [
    {
      jobId, jobTitle, company, seniority,
      bi_score, cross_encoder_score, final_score,
      fit_narrative, key_gap,
      rank (1–5)
    }
  ]
  generatedAt: ISO timestamp
  basedOnCvKey: file_key  ← so frontend knows which CV version this is based on
```

---

### File 2: `candidate_ranking_engine.py`

**Lambda name:** `smarthire-candidate-ranking-engine`  
**Trigger:** SNS topic `smarthire-job-published` → SQS → Lambda  
**Why SNS and not Step Functions?** The JD posting flow is synchronous from the recruiter's perspective (they get 202 Accepted immediately). The ranking happens async after jd_parser completes. SNS decouples this from the JD parsing workflow entirely.

**Trigger setup (not your Lambda code, but document for the team):**  
In `jd_parser.py`, at the end of a successful `table.put_item()`:

```python
# Add this to jd_parser.py after the DynamoDB write succeeds:
if JOB_PUBLISHED_TOPIC_ARN := os.environ.get("JOB_PUBLISHED_TOPIC_ARN"):
    sns_client.publish(
        TopicArn=JOB_PUBLISHED_TOPIC_ARN,
        Message=json.dumps({
            "jobId": str(job_id),
            "jobTitle": final_job_title,
            "jdVector": [float(v) for v in item.get("jdVector", [])],
            "jdText": cleaned_jd_text[:2000],
            "requiredSkills": structured_jd.get("required_skills", []),
        }),
        Subject="JOB_PUBLISHED",
    )
```

**Input event** (from SQS → SNS message body):

```json
{
  "jobId": "job-uuid",
  "jobTitle": "Backend Engineer",
  "jdVector": [0.11, 0.38, ...],
  "jdText": "We are looking for a backend engineer with 3+ years...",
  "requiredSkills": ["Python", "AWS Lambda", "PostgreSQL"]
}
```

**Implementation steps:**

```
Step 1 — pgvector kNN Search (top 50 candidates)
    Query: SELECT c.user_id, c.seniority_estimate, c.years_experience,
                  c.matching_score, c.extracted_profile,
                  (e.embedding <-> jd_vector) AS dist
            FROM candidate_embeddings e
            JOIN candidate_profiles_ai c ON e.user_id = c.user_id
            ORDER BY dist ASC
            LIMIT 50;
    
    Note: candidate_profiles_ai has the parsed_data (skills, seniority, etc.).
    You need both the vector distance AND the structured profile for the
    subsequent Cross-Encoder and Claude steps.

Step 2 — Cross-Encoder Reranking (top 50 → top 10)
    For each of the 50 candidates, retrieve masked_cv_text from candidate_embeddings.raw_text.
    
    Pairs: [(jd_text[:1500], candidate_raw_text_chunk)]
    Apply same chunking + weighted top-K average pattern from vector_ops.py.
    
    Re-sort → top 10 by cross_encoder_score.
    
    IMPORTANT: Do NOT run all 50 through the Cross-Encoder sequentially.
    Use batched predict(): cross_encoder.predict(all_pairs_list) returns a
    numpy array in one call. The model handles batching internally.
    Build all pairs first, then call predict() once with the full batch.

Step 3 — Claude Agent 4: Per-Candidate Recruiter Summary (top 10 in parallel)
    System: "You are a Hiring Assistant AI. Given a job description and a candidate profile,
             write a 2-sentence recruiter briefing that explains:
             1. The strongest evidence this candidate can do the job.
             2. The one question a recruiter MUST ask them to verify fit.
             Output JSON: { 'recruiter_summary': '...', 'must_ask_question': '...' }"
    
    User:   "Job: {jobTitle}, Required skills: {requiredSkills}
             Candidate seniority: {seniority}, Years exp: {years_experience}
             Candidate skills: {all_skills_from_extracted_profile}
             Gaps: {gaps field from extracted_profile}"
    
    Run all 10 calls in parallel using ThreadPoolExecutor(max_workers=10).

Step 4 — Write to DynamoDB (recruiter dashboard data)
    PK = JOB#{jobId}
    SK = RANKED_CANDIDATES
    GSI1PK = "RECRUITER_DASHBOARD"
    GSI1SK = updatedAt
    TTL = 14 days

    Fields:
      rankedCandidates: [
        {
          rank (1–10),
          candidateId,
          seniority,
          years_experience,
          bi_score,
          cross_encoder_score,
          final_score,           ← weighted blend same as job_suggestion_engine
          recruiter_summary,     ← Claude Agent 4 output
          must_ask_question,     ← Claude Agent 4 output
          top_matching_skills,   ← intersection of requiredSkills and candidate skills
          key_gaps               ← missing required skills
        }
      ]
      jobId, jobTitle
      generatedAt
      totalCandidatesScanned   ← how many candidates were in pgvector at query time
```

---

### File 3: `suggestion_score_utils.py`

**Purpose:** Shared scoring utilities used by BOTH `job_suggestion_engine.py` and `candidate_ranking_engine.py`. Do NOT duplicate the Cross-Encoder logic into two files.

**Why a separate shared module?** Both engines use:
1. `_get_cross_encoder()` singleton
2. `_create_chunks()` with overlap
3. `_aggregate_chunk_scores()` weighted top-K
4. `_sigmoid()` logit normalization
5. `compute_hybrid_score()` blending

These are currently in `vector_ops.py`. They need to live in a Lambda Layer or a shared utility module imported by both engines.

**Deployment strategy:** Package `suggestion_score_utils.py` as a Lambda Layer artifact:

```
layers/
  smarthire-scoring-utils/
    python/
      suggestion_score_utils.py    ← your shared module
      (sentence_transformers here too, baked as a layer or container)
```

Both `job_suggestion_engine` and `candidate_ranking_engine` import it as:

```python
from suggestion_score_utils import (
    get_cross_encoder,
    create_cv_chunks,
    compute_cross_encoder_score,
    compute_hybrid_score,
)
```

**Functions to include:**

```python
# suggestion_score_utils.py
# All functions migrated from vector_ops.py, made import-safe (no boto3 clients here):

def get_cross_encoder() -> Any | None: ...              # Singleton loader
def _sigmoid(x: float) -> float: ...                    # logit → [0,1]
def create_cv_chunks(text: str, ...) -> list[str]: ...  # Overlapping windows
def aggregate_chunk_scores(scores: list[float]) -> float: ...  # Weighted top-K
def compute_cross_encoder_score(cv_text: str, jd_text: str) -> float | None: ...
def compute_hybrid_score(bi_pct: float, cross: float | None) -> tuple[float, dict]: ...
```

---

## 4. Step Functions: New State Machine (PROFILE_UPLOAD workflow)

This is the new state machine for Flow 1. The existing state machine (JD_APPLICATION) remains unchanged.

**State machine name:** `SmartHire-ProfileUpload-Workflow`

```json
{
  "Comment": "Global CV profile upload: extract, embed, suggest matching jobs",
  "StartAt": "TextProcessorTask",
  "States": {

    "TextProcessorTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${TextProcessorFunction}",
        "Payload.$": "$"
      },
      "ResultPath": "$.text_processor_output",
      "Next": "VectorOpsTask",
      "Retry": [{ "ErrorEquals": ["Lambda.ServiceException"], "MaxAttempts": 2, "IntervalSeconds": 3 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "MarkFailed" }]
    },

    "VectorOpsTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${VectorOpsFunction}",
        "Payload": {
          "payload.$": "$.text_processor_output.Payload",
          "flow_mode": "PROFILE_UPLOAD"
        }
      },
      "ResultPath": "$.vector_ops_output",
      "Next": "JobSuggestionTask",
      "Retry": [{ "ErrorEquals": ["Lambda.ServiceException"], "MaxAttempts": 2, "IntervalSeconds": 5 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "MarkFailed" }]
    },

    "JobSuggestionTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${JobSuggestionEngineFunction}",
        "Payload.$": "$.vector_ops_output.Payload"
      },
      "ResultPath": "$.suggestion_output",
      "Next": "VectorPersisterTask",
      "Retry": [{ "ErrorEquals": ["Lambda.ServiceException"], "MaxAttempts": 1, "IntervalSeconds": 5 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "VectorPersisterTask" }]
    },

    "VectorPersisterTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${VectorPersisterFunction}",
        "Payload": {
          "profile_id.$": "$.vector_ops_output.Payload.profile_id",
          "file_key.$": "$.vector_ops_output.Payload.file_key",
          "job_id.$": "$.vector_ops_output.Payload.job_id",
          "masked_cv_text.$": "$.vector_ops_output.Payload.masked_cv_text",
          "cv_vector.$": "$.vector_ops_output.Payload.cv_vector",
          "parsed_data.$": "$.vector_ops_output.Payload.parsed_data",
          "scoring_details.$": "$.vector_ops_output.Payload.scoring_details",
          "interview_guide.$": "$.vector_ops_output.Payload.interview_guide",
          "masking_report.$": "$.vector_ops_output.Payload.masking_report",
          "jd_text.$": "$.vector_ops_output.Payload.jd_text",
          "jd_vector.$": "$.vector_ops_output.Payload.jd_vector",
          "job_suggestions.$": "$.suggestion_output.Payload.job_suggestions"
        }
      },
      "End": true,
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "MarkFailed" }]
    },

    "MarkFailed": {
      "Type": "Task",
      "Resource": "arn:aws:states:::dynamodb:putItem",
      "Parameters": {
        "TableName": "${DynamoDBTable}",
        "Item": {
          "PK": { "S.$": "States.Format('CANDIDATE#{}', $$.Execution.Input.profile_id)" },
          "SK": { "S": "PROCESSING_FAILURE" },
          "parseStatus": { "S": "FAILED" },
          "updatedAt": { "S.$": "$$.State.EnteredTime" }
        }
      },
      "End": true
    }
  }
}
```

Note: The `Catch` on `JobSuggestionTask` goes to `VectorPersisterTask` (not `MarkFailed`). Suggestion engine failure is non-fatal — the CV is still stored and searchable.

---

## 5. DynamoDB Access Patterns Summary

These are all the read/write patterns the frontend team will query. Your Lambda outputs must match these exactly.

| PK | SK | Written by | Purpose |
|---|---|---|---|
| `CANDIDATE#{id}` | `CV#{fileKey}` | `vector_persister` | Per-file parse result (skills, scoring) |
| `CANDIDATE#{id}` | `JOB_SUGGESTIONS` | `vector_persister` (via suggestion engine) | Suggested jobs for candidate dashboard |
| `JOB#{jobId}` | `RANKED_CANDIDATES` | `candidate_ranking_engine` | Ranked candidates for recruiter dashboard |
| `CANDIDATE#{id}` | `CV#{fileKey}` | `vector_persister` (on application) | Per-JD match score when candidate applies |

**GSI1 for listing queries:**
- `GSI1PK = "JOB_SUGGESTIONS"` → fetch all candidates who have fresh suggestions (admin view)
- `GSI1PK = "RECRUITER_DASHBOARD"` → fetch all jobs that have ranked candidate lists

---

## 6. Implementation Order (Strict Sequence)

Build in this order. Each step unblocks the next.

### Week 1

**Day 1–2:** `suggestion_score_utils.py`
- Migrate `_get_cross_encoder`, `_sigmoid`, `create_cv_chunks`, `aggregate_chunk_scores`, `compute_cross_encoder_score`, `compute_hybrid_score` from `vector_ops.py` to this shared module
- Add unit tests for each function (pytest, no AWS mocks needed — these are pure math)
- Package as Lambda Layer

**Day 3–4:** Patches to existing files
- Patch `ingestion_trigger.py`: add `flow_mode` to payload, add `PROFILE_STATE_MACHINE_ARN` routing
- Patch `vector_ops.py`: propagate `flow_mode` in return payload
- Patch `vector_persister.py`: rename `topMatches` → `jobSuggestions`, accept `job_suggestions` in input, add `save_job_suggestions` branch

**Day 5:** Step Functions state machine (PROFILE_UPLOAD)
- Deploy the new JSON definition via Terraform or SAM
- Test with a stub `job_suggestion_engine` that returns a hardcoded response
- Confirm end-to-end payload flow through all Lambda hops

### Week 2

**Day 1–3:** `job_suggestion_engine.py`
- pgvector kNN query (reuse connection pattern from `vector_persister.py`)
- Cross-Encoder reranking using `suggestion_score_utils`
- Claude Agent 3 parallel narrative generation with `ThreadPoolExecutor`
- Full integration test: upload a real CV with no jobId, verify DynamoDB write

**Day 4–5:** `candidate_ranking_engine.py`
- SNS → SQS trigger setup (infrastructure)
- pgvector kNN from JD vector
- Batched Cross-Encoder predict()
- Claude Agent 4 parallel recruiter summaries
- DynamoDB write with full `rankedCandidates` schema

---

## 7. Critical Technical Notes

### On Parallel Bedrock Calls (Claude Agent 3 + 4)

Use `concurrent.futures.ThreadPoolExecutor`, not `asyncio`. Boto3 is not async-compatible. Pattern:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def generate_narratives_parallel(candidates: list[dict], job_context: dict) -> list[dict]:
    results = [None] * len(candidates)
    
    def process_one(index_and_candidate):
        idx, candidate = index_and_candidate
        try:
            narrative = _call_claude_for_narrative(candidate, job_context)
            return idx, narrative
        except Exception as e:
            print(f"WARNING: Narrative failed for candidate {candidate['candidateId']}: {e}")
            return idx, {"recruiter_summary": "Analysis unavailable", "must_ask_question": ""}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_one, item): item for item in enumerate(candidates)}
        for future in as_completed(futures):
            idx, narrative = future.result()
            results[idx] = {**candidates[idx], **narrative}
    
    return [r for r in results if r is not None]
```

### On pgvector Connection Management

Both engines run in VPC. Reuse the `_resolve_rds_credentials()` + `pg8000.native.Connection` pattern from `vector_persister.py` verbatim. Do NOT use psycopg2 unless pg8000 is unavailable — pg8000 is pure Python and ships inside the Lambda artifact without native binary dependencies.

### On Cross-Encoder Batch Size

When ranking 50 candidates in `candidate_ranking_engine.py`, each candidate produces up to 12 chunks (max). That's up to 600 pairs sent to `ce.predict()` in one batch call. The MiniLM model handles this fine (it's 22MB, CPU inference). Expected wall time: ~4–6 seconds for 600 pairs on a 1GB Lambda. Set `candidate_ranking_engine` memory to **1024MB** and timeout to **120 seconds**.

For `job_suggestion_engine.py`, only 20 jobs × 12 chunks = 240 pairs max. Set memory to **512MB**, timeout to **60 seconds**.

### On Lambda Payload Size Limit (6MB for sync, 256KB for async)

`cv_vector` is a list of 1024 floats ≈ 8KB. `masked_cv_text` truncated to 50,000 chars ≈ 50KB. The full payload between Step Functions states stays well under 256KB. If you hit limits: store `masked_cv_text` in S3 and pass the S3 key instead of the full text. Add an env flag `USE_S3_PAYLOAD=true` for large documents.

### On Scoring Consistency

Both engines must use identical hybrid weights (`HYBRID_WEIGHT_BI=0.35`, `HYBRID_WEIGHT_CROSS=0.65`) from `suggestion_score_utils`. If the weights differ between engines, a candidate's score for a job will change depending on whether the score was computed from the CV side or the JD side — that inconsistency will confuse recruiters. The shared module enforces this.

---

## 8. Deliverables Checklist

| Item | File | Done? |
|---|---|---|
| Shared scoring utils | `suggestion_score_utils.py` | ⬜ |
| Patch: flow_mode routing | `ingestion_trigger.py` | ⬜ |
| Patch: flow_mode propagation | `vector_ops.py` | ⬜ |
| Patch: jobSuggestions schema | `vector_persister.py` | ⬜ |
| New Step Functions (PROFILE_UPLOAD) | `profile_workflow.json` | ⬜ |
| Job suggestion engine | `job_suggestion_engine.py` | ⬜ |
| Candidate ranking engine | `candidate_ranking_engine.py` | ⬜ |
| SNS publish in jd_parser | `jd_parser.py` patch | ⬜ |
| Unit tests for scoring utils | `tests/test_scoring_utils.py` | ⬜ |
| Integration test: global profile upload | manual / pytest with moto | ⬜ |
| Integration test: JD published → ranking | manual / pytest with moto | ⬜ |
