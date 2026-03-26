# SmartHire Matching System Architecture

## Overview

This document details the architecture for the **Smart JD Matching System**, enabling real-time, bi-directional matching between Candidates and Jobs using **PostgreSQL (pgvector)** and **AWS Step Functions**.

---

## 1. System Workflows (Sequence Diagrams)

This section consolidates all interaction flows for Candidates, Recruiters, and Application Management.

### A. Candidate Flow (Upload -> Match)

```mermaid
sequenceDiagram
    autonumber
    participant C as Candidate (Browser)
    participant S3 as S3 (Raw Bucket)
    participant SQS as SQS (cv-upload-queue)
    participant L1 as Lambda (IngestionTrigger)
    participant SF as Step Functions
    participant SV_Text as Textract/Comprehend
    participant SV_AI as Bedrock (Claude)
    participant L2 as Lambda (VectorOps)
    participant SV_Embed as Bedrock (Cohere)
    participant RDS as RDS Proxy (pgvector)
    participant DDB as DynamoDB (Hot Store)
    participant APP as AppSync (Realtime)

    Note over C, L1: 1. Ingestion Phase
    C->>S3: Upload CV.pdf
    S3->>SQS: Push Event (s3:ObjectCreated)
    SQS->>L1: Trigger (Batch of 10)

    rect rgb(240, 248, 255)
        Note right of L1: Lambda 1: Starter
        L1->>L1: Validate File Size/Type
        L1->>SF: StartExecution (path, userId)
    end

    Note over SF, SV_AI: 2. Analysis Phase (Orchestration)
    SF->>SV_Text: Textract: AnalyzeDocument (Async)
    SV_Text-->>SF: Raw Text Blocks
    SF->>SV_Text: Comprehend: DetectPII
    SV_Text-->>SF: Redacted Text
    SF->>SV_AI: InvokeModel (Claude 3) <br/>"Extract Skills & Exp to JSON"
    SV_AI-->>SF: Structured Profile JSON

    Note over SF, APP: 3. Vector & Match Phase
    SF->>L2: Invoke "VectorOps" (Profile + RedactedText)

    rect rgb(255, 250, 240)
        Note right of L2: Lambda 2: Matcher
        L2->>SV_Embed: Generate Embedding (Cohere v3)
        SV_Embed-->>L2: Vector [0.12, 0.45...]
        L2->>RDS: INSERT Candidate Profile + Vector
        L2->>RDS: SELECT closest Jobs (L2 distance)
        RDS-->>L2: Top 10 Job Matches
        L2->>DDB: PutItem "MatchResult"
        L2->>APP: Mutation "PublishMatch"
    end

    APP-->>C: WebSocket Subscription Update
```

### B. Recruiter Flow (Job Post -> Candidate Rank)

```mermaid
sequenceDiagram
    autonumber
    participant R as Recruiter
    participant API as API Gateway
    participant SF as Step Functions
    participant SV_AI as Bedrock (Claude)
    participant L2 as Lambda (VectorOps)
    participant SV_Embed as Bedrock (Cohere)
    participant RDS as RDS Proxy (pgvector)
    participant DDB as DynamoDB

    Note over R, API: 1. Job Creation
    R->>API: POST /jobs (Description)
    API->>SF: Start "JD Processing" Workflow
    API-->>R: 202 Accepted (Processing)

    Note over SF, SV_AI: 2. Analysis (Standardized)
    SF->>SV_AI: InvokeModel (Claude) <br/>"Standardize Requirements"
    SV_AI-->>SF: JSON (Required Skills, Level)

    Note over SF, RDS: 3. Vectorize & Candidate Search
    SF->>L2: Invoke "VectorOps" (JD Text)

    rect rgb(255, 250, 240)
        Note right of L2: Lambda 2: Logic
        L2->>SV_Embed: Generate Embedding (Cohere)
        L2->>RDS: INSERT Job Vector
        L2->>RDS: SELECT top 50 Candidates <br/>(WHERE skills overlap)
        RDS-->>L2: Candidate List
    end
    L2-->>SF: List of 50 Candidates

    Note over SF, DDB: 4. AI Scoring Loop
    loop For each Candidate (Parallel Map)
        SF->>SV_AI: Compare (JD vs CV) -> Score 0-100
    end

    SF->>DDB: BatchWrite "RecommendedCandidates"
    SF->>DDB: Update "JobStats" (Views/Matches)
    SF->>DDB: Update "RecruiterDashboard"
```

### C. Application Management Flow (.NET Core CRUD)

```mermaid
sequenceDiagram
    autonumber
    participant C as Candidate (Frontend)
    participant R as Recruiter
    participant NET as .NET API (Backend)
    participant RDS as RDS (Metadata)

    Note over C, NET: 1. Candidate Applies
    C->>NET: POST /api/applications { jobId }

    rect rgb(240, 248, 255)
        Note right of NET: .NET Controller Logic
        NET->>RDS: Validate User & Job Exist
        NET->>RDS: Check for Duplicate Application
        NET->>RDS: INSERT INTO Applications (Status="APPLIED")
    end

    NET-->>C: 201 Created { applicationId, status: "APPLIED" }

    Note over R, NET: 2. Recruiter Review (CRUD)
    R->>NET: GET /api/jobs/{jobId}/applications
    NET->>RDS: SELECT Applications JOIN Users JOIN Profiles
    RDS-->>NET: Application List [ { id, candidateName, matchScore } ]
    NET-->>R: 200 OK (List)

    Note over R, NET: 3. Status Update
    R->>NET: PATCH /api/applications/{appId} { status: "INTERVIEWING" }
    NET->>RDS: UPDATE Applications SET Status="INTERVIEWING"
    NET-->>R: 200 OK
```

---

## 2. Data Models & Entity Relationships (ERD)

### A. Unified Database Schema

```mermaid
erDiagram
    Users ||--|| CandidateProfiles : "has_profile"
    Users ||--|| CandidateEmbeddings : "has_vector"
    Companies ||--|{ Jobs : "posts"
    Jobs ||--|| JobEmbeddings : "has_vector"
    Users ||--|{ Applications : "applies_to"
    Jobs ||--|{ Applications : "receives"

    Users {
        int id PK
        string email
        string cognito_sub
        string user_type
    }

    CandidateProfiles {
        int id PK
        int user_id FK
        jsonb extracted_profile "skills, exp, education, summary"
        timestamp updated_at
    }

    CandidateEmbeddings {
        int id PK
        int user_id FK
        vector(1024) embedding
    }

    Companies {
        int id PK
        string name
        string industry
    }

    Jobs {
        int id PK
        int company_id FK
        string title
        text description
        jsonb required_skills
        boolean is_active
    }

    JobEmbeddings {
        int id PK
        int job_id FK
        vector(1024) embedding
    }

    Applications {
        int id PK
        int user_id FK
        int job_id FK
        string status "APPLIED|REVIEWING|REJECTED"
        float match_score
        timestamp created_at
    }
```

### B. Data Storage Strategy

#### 1. Raw Assets (Amazon S3)

- **Bucket**: `smarthire-raw-assets`
- **Paths**: `candidates/{userId}/cv.pdf`, `jobs/{jobId}/jd.pdf`
- **Purpose**: Immutable backup and audit trail.

#### 2. Relational & Vector Data (Amazon RDS PostgreSQL)

Serves as the **Source of Truth** for business entities and semantic headers.

- **Tables**: `CandidateProfiles`, `Jobs`, `Applications`.
- **Pgvector**: `CandidateEmbeddings`, `JobEmbeddings`.

#### 3. Hot Store / Cache (Amazon DynamoDB)

- **Table**: `ApplicationTracking` (Single Table Design)
- **Purpose**: High-frequency read/write operations for realtime UI updates (e.g., dashboard stats).

---

## 3. Component Details & Payload Logic

To decouple ingestion from heavy processing, we use **SQS** as a buffer and **Step Functions** for orchestration.

### A. SQS Queue (`cv-upload-queue`)

- **Trigger**: S3 Event Notification (`s3:ObjectCreated:Put`).
- **Payload**: Standard S3 Event JSON.

### B. Lambda 1: `IngestionTrigger` (The "Starter")

- **Responsibility**: Validates file, Starts Step Function.

### C. Step Functions Orchestration

1.  **Textract**: Reads PDF.
2.  **Comprehend**: Detects PII.
3.  **Bedrock (Claude)**: Extracts structured JSON.

### D. Lambda 2: `VectorOps` (The "Matcher")

- **Responsibility**: Embeds text (Cohere), updates RDS (pgvector), syncs to DynamoDB.

---

## 4. Hybrid Architecture Strategy

### A. The "Split Brain" Design (Net vs Python)

| Feature         | Technology Stack        | Responsibility                                 | Type                        |
| :-------------- | :---------------------- | :--------------------------------------------- | :-------------------------- |
| **User/Auth**   | .NET 8 Lambda + Cognito | Auth, Profiles, Job CRUD, Application Tracking | Synchronous (API GW)        |
| **File Upload** | AWS S3 Signed URLs      | Secure direct-to-bucket transfers              | Synchronous (Client <-> S3) |
| **AI/Vector**   | Python + Step Functions | Parsing, Embedding, Vector Search              | Asynchronous (Event-Driven) |

### B. API Contracts (CRUD)

**1. Create Application**
`POST /api/applications`

```json
{ "jobId": 102, "notes": "..." }
```

**2. Get Job Applications**
`GET /api/jobs/102/applications`

---

## 5. Infrastructure Migration Plan (IaC)

### A. What to **DELETE** (Refactor)

- `iac/lambda/cv_parser/cv_parser.py`
- `iac/lambda/jd_parser/jd_parser.py`

### B. What to **ADD** (New Resources)

- **SQS Queues**: `iac/modules/queue`
- **Step Functions**: `iac/modules/processing`
- **Lambda (Ingest & VectorOps)**
- **RDS Proxy**
- **AppSync**
- **X-Ray**: Service instrumentation and permissions.

### C. Terraform Variable Changes

---

## 6. Observability & Monitoring (AWS X-Ray)

To ensure visibility into the CV/JD processing pipeline, we will implement **AWS X-Ray** for end-to-end tracing of the asynchronous AI workflows.

### A. Tracing Scope

X-Ray instrumentation applies **only** to the CV/JD processing flows (Candidate Flow and Recruiter Flow):

1.  **Lambda 1 (IngestionTrigger)**: Trace file validation and Step Function invocation.
2.  **Step Functions Workflow**: Visualize orchestration path (Textract → Comprehend → Bedrock → VectorOps).
3.  **Lambda 2 (VectorOps)**: Trace embedding generation, RDS writes, and DynamoDB updates.
4.  **Downstream Services**: Capture sub-segments for Textract, Bedrock, RDS, and DynamoDB calls.

### B. Key Metrics to Monitor

- **End-to-End Latency**: Time from CV upload to embedding completion and DynamoDB sync.
- **Error Rates**: Failures in Textract, Bedrock throttling, or database timeouts.
- **Service Dependencies**: Visualize how Lambdas, Step Functions, and AI services interact.
- **Cold Starts**: Lambda initialization impact on ingestion throughput.

### C. Implementation Details

- **IAM Permissions**: Add `xray:PutTraceSegments` and `xray:PutTelemetryRecords` to Lambda execution roles.
- **Environment Variables**: Set `AWS_XRAY_CONTEXT_MISSING` to `LOG_ERROR` to prevent trace errors.
- **Sampling Rules**: Configure rules in X-Ray console (e.g., 100% sampling for errors, 10% for successful CV parses).

- `textract_enabled`
- `bedrock_model_id`
- `rds_proxy_endpoint`

```

```
