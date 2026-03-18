**SmartHire AI**

AI-Powered Technical Recruitment Platform

_PROJECT BLUEPRINT · 1 Month · 4 Sprints · 5 Members_

| Amazon Web Services | 4-Week Timeline | 100% Serverless | 5 Members |
| :-----------------: | :-------------: | :-------------: | :-------: |

# **TABLE OF CONTENTS**

| 1\.     | Project Overview                             |   3 |
| :------ | :------------------------------------------- | --: |
| 1.1     | Background & Business Value                  |   3 |
| 1.2     | Core Workflow (4 Stages)                     |   4 |
| 1.3     | Technical Value Proposition                  |   4 |
| **2\.** | **Architecture & Tech Stack**                |   5 |
| 2.1     | Architecture Overview                        |   5 |
| 2.2     | AWS Services Breakdown                       |   5 |
| 2.3     | Data Flow by Stage                           |   7 |
| **3\.** | **Detailed Role Assignments**                |   9 |
| 3.1     | Backend 1 — System Architect & API Lead      |   9 |
| 3.2     | Backend 2 — WebSocket & Pipeline Engineer    |  10 |
| 3.3     | Frontend 1 — Dashboard & DevOps Lead         |  11 |
| 3.4     | Frontend 2 — Interview Room & Media Engineer |  12 |
| 3.5     | AI Engineer — Brain & Intelligence Layer     |  13 |
| **4\.** | **4-Week Sprint Plan**                       |  14 |
| **5\.** | **Requirements Matrix & Risk Management**    |  16 |
| **6\.** | **Non-Functional Requirements (NFR)**        |  17 |

# **1\. Project Overview**

## **1.1 Background & Business Value**

The technical hiring process is fundamentally broken for both sides. Recruiters spend 3-5 hours per candidate on manual screening and interviews, while candidates receive little structured feedback. Existing platforms (HackerRank, CoderPad, Greenhouse) are either prohibitively expensive for SMEs or lack meaningful AI evaluation \- they check if code runs, not whether the candidate understands why it runs.

SmartHire solves this by building a complete end-to-end AI-powered technical interview platform: affordable (serverless pay-per-use, \~$0.08/session), deeply evaluative (voice Q\&A \+ live coding \+ emotion analysis), and personalized (RAG pipeline customizes questions per Job Description). Estimated outcome: 60% reduction in recruiter screening effort, actionable scorecard for every candidate.

| PAIN POINT                                     | SMARTHIRE SOLUTION                                                                               |
| :--------------------------------------------- | :----------------------------------------------------------------------------------------------- |
| 3-5 hrs/candidate manual screening             | AI auto-grades coding \+ Q\&A; recruiter reviews a 15-minute structured report                   |
| Inconsistent evaluation between interviewers   | Standardized STAR rubric applied by AI uniformly across all candidates                           |
| Expensive SaaS licenses, unaffordable for SMEs | Pay-per-interview serverless model: \~$0.08/45-min session, $0 idle cost                         |
| Generic question banks, not JD-specific        | Bedrock RAG generates role-specific questions from the uploaded Job Description                  |
| No feedback loop for candidates                | Detailed scorecard: technical score, communication score, skill gap analysis, learning resources |

## **1.2 Core Workflow (4 Stages)**

SmartHire operates through four clearly defined stages that demonstrate a complete cloud-native pipeline from data ingestion to AI-powered output:

- Stage 1 \- SETUP: Recruiter creates a job and uploads the Job Description. Candidates upload their CV, parsed by Amazon Textract \+ Bedrock Claude to extract and categorize skills. Bedrock Knowledge Base indexes the JD for personalized question generation specific to this role.

- Stage 2 \- INTERVIEW (Core): Candidate enters the virtual interview room. Live coding editor (Monaco) and voice Q\&A run in parallel. Backend streams audio via API Gateway WebSocket to Amazon Transcribe to Bedrock Claude 3.5 to Amazon Polly to client, targeting under 1.5 seconds end-to-end roundtrip.

- Stage 3 \- ANALYSIS (Async): A parallel SQS-driven pipeline processes webcam frames via Amazon Rekognition for emotion and attention analysis. Code submissions are evaluated in an isolated ECS Fargate sandbox. Both pipelines are fully decoupled from the real-time session.

- Stage 4 \- REPORT: Session ends. EventBridge triggers a Step Functions workflow. Bedrock aggregates and scores all answers against the STAR rubric. Generates a structured scorecard (Technical 40%, Communication 30%, Problem-Solving 30%). Report stored in S3. SNS notifications to recruiter and candidate dashboard.

## **1.3 Technical Value Proposition**

SmartHire demonstrates cloud-native engineering competency at Junior level with a monolithic serverless backend, aligned with AWS foundation requirements:

- Serverless-First: single ASP.NET backend on ECS Fargate \+ API Gateway \+ ALB \+ managed data services, zero fixed servers

- Event-Driven Architecture: S3 to SQS to workers; EventBridge triggers post-interview workflows without tight coupling

- Multi-AI Pipeline: Transcribe \+ Bedrock (Claude 3.5) \+ Polly \+ Rekognition \+ Textract integrated in one end-to-end system

- Monolithic Backend: one .NET service handles all REST and WebSocket orchestration, shared domain models, and unified data access

- Security (CIS aligned): Cognito OAuth 2.0, WAF, KMS, Secrets Manager, IAM least privilege, CloudTrail, Security Hub, Config rules

- Observability: CloudWatch custom metrics and dashboards, X-Ray tracing, alarms to SNS for latency and error spikes

- Infrastructure as Code: 100% Terraform with environment parameterization, blue/green rollback on ECS deploys

# **2\. Architecture & Tech Stack**

## **2.1 Architecture Overview**

SmartHire is a monolithic serverless backend. A single ASP.NET service will run on ECS Fargate and own all API routes, business logic, and orchestration. It uses managed AWS services for storage, AI, and async processing while keeping one unified codebase and data model.

> **IaC Status:** The current Terraform IaC has deployed the **foundation layer** — networking, auth, database, frontend delivery, and CI/CD. The compute and AI layers (ECS, API Gateway, DynamoDB, SQS, Bedrock, etc.) are **planned for Sprints 2–4**.

**Current Deployed Flow (IaC Accurate):**

```
React SPA (Browser)
  → Route 53 → CloudFront (WAF + OAC) → S3 (static files)          [Frontend]
  → Cognito SDK direct (OAuth 2.0 + Google SSO)                     [Auth]
    → PostConfirmation trigger → Lambda (private subnet)
      → Secrets Manager VPC Endpoint → RDS credentials
      → RDS PostgreSQL (INSERT Users, private subnet)               [DB Sync]
```

**Full Target Flow (incl. planned services):**

```
[ ReactJS + Tailwind ] --> CloudFront + S3 (Frontend Host)
  --> Cognito (direct SDK auth) --> JWT token
  --> API Gateway (REST + WebSocket) --> VPC Link --> ALB
  --> ECS Fargate (ASP.NET Monolith)
      --> RDS PostgreSQL + DynamoDB + S3
      --> Transcribe + Bedrock + Polly + Rekognition
      --> SQS --> Workers --> EventBridge --> Step Functions
```

## **2.2 AWS Services Breakdown**

> ✅ = **Deployed in current IaC** · 🔲 = **Planned (Sprint 2–4)**

| EDGE & DELIVERY LAYER                    |                                                                                                                  | Status |
| :--------------------------------------- | :--------------------------------------------------------------------------------------------------------------- | :----: |
| **CloudFront + S3 (Frontend)**           | React SPA CDN delivery; S3 private bucket (block public access ON); OAC signed requests; TLS 1.2+ enforced        | ✅ |
| **Route 53**                             | A-record alias to CloudFront for `smarthire-ai.org` and `www.smarthire-ai.org`; DNS-validated ACM cert (us-east-1) | ✅ |
| **AWS WAF**                              | Existing WAF web ACL attached to CloudFront; SQL injection, XSS, IP rate-limiting rules                           | ✅ |
| **API Gateway \- REST API**              | Unified API for Jobs, Sessions, Reports with JWT validation, rate limiting and caching                            | 🔲 |
| **API Gateway \- WebSocket API**         | Persistent bidirectional channel: audio stream, live code sync, AI response push                                  | 🔲 |
| **AUTH — Amazon Cognito**                |                                                                                                                  | ✅ |
| **Cognito User Pool**                    | Email + password auth; Google SSO via Identity Provider; custom:role attribute (RECRUITER / CANDIDATE)            | ✅ |
| **Cognito App Client**                   | OAuth 2.0 authorization code flow, PKCE; no client secret; frontend SDK connects directly (no API Gateway hop)    | ✅ |
| **Lambda — cognito\_post\_confirmation** | Node.js 20; runs in **private DB subnet**; triggered by Cognito on signup confirmation; upserts user into RDS    | ✅ |
| **NETWORKING — VPC**                     |                                                                                                                  | ✅ |
| **VPC (10.0.0.0/16)**                    | DNS hostnames + support enabled; 2 public subnets + 2 private DB subnets across 2 AZs                            | ✅ |
| **Internet Gateway + Public Route**      | Public subnets route 0.0.0.0/0 to IGW; private subnets have no route to internet                                 | ✅ |
| **VPC Endpoint — Secrets Manager**       | Interface endpoint in private subnets; lets Lambda reach Secrets Manager **without NAT Gateway**                  | ✅ |
| **VPC Endpoint — KMS**                   | Interface endpoint in private subnets; lets Lambda call KMS for decryption without internet                       | ✅ |
| **Security Groups**                      | `app-sg` (Lambda/app), `rds-sg` (DB, ingress from app-sg only), `bastion-sg` (SSH from configured CIDR)          | ✅ |
| **DATA & STORAGE**                       |                                                                                                                  |        |
| **RDS PostgreSQL 15**                    | Single-AZ, gp3, encrypted (KMS), private subnet only; IAM DB auth enabled; backup 7 days; CloudWatch logs on     | ✅ |
| **Secrets Manager — RDS credentials**    | Stores host/user/password/port/dbname; KMS encrypted; Lambda fetches via VPC endpoint at cold start              | ✅ |
| **KMS Key**                              | Auto-rotation ON; alias `smarthire-secrets-*`; encrypts RDS secret in Secrets Manager                            | ✅ |
| **Bastion Host (EC2 t3.micro)**          | Amazon Linux 2023; public subnet; Elastic IP; SSH port 22 from admin CIDR; PostgreSQL 15 client installed         | ✅ |
| **DynamoDB — Sessions & Interviews**     | Active session state, WebSocket connectionIds, Q&A, emotion scores; on-demand capacity                            | 🔲 |
| **ElastiCache Redis**                    | JWT session cache, rate limit counters, WebSocket state; sub-millisecond lookups                                  | 🔲 |
| **S3 — Recordings & Reports**            | Audio/video (SSE-KMS); pre-signed URLs; Glacier lifecycle after 30 days; CV + report storage                      | 🔲 |
| **COMPUTE — SERVERLESS MONOLITH**        |                                                                                                                  |        |
| **ECS Fargate — ASP.NET Monolith**       | Single service: all REST + WebSocket routes, auth, jobs, sessions, AI orchestration, report trigger               | 🔲 |
| **ECS Fargate — Code Executor**          | Sandboxed containers per language (Node / Python / Java); 30s hard timeout; isolated per submission               | 🔲 |
| **ALB (Application Load Balancer)**      | Routes traffic from API Gateway VPC Link to ECS tasks; health checks                                             | 🔲 |
| **AI & ML SERVICES**                     |                                                                                                                  |        |
| **Amazon Bedrock — Claude 3.5 Sonnet**   | Core LLM: interview dialogue, question generation, STAR scoring, report narrative                                 | 🔲 |
| **Bedrock Knowledge Base (RAG)**         | Vector embeddings from JD files; 512-token chunks; Titan Embeddings; personalized per job                         | 🔲 |
| **Amazon Transcribe (Streaming)**        | Real-time speech-to-text; partial results for < 1.5s voice loop latency                                           | 🔲 |
| **Amazon Polly (Neural TTS)**            | AI voice; streamed MP3 audio back to client over WebSocket                                                        | 🔲 |
| **Amazon Rekognition**                   | Facial emotion per frame: CALM / CONFUSED / FEAR confidence scores + FacePose (eye contact proxy)                 | 🔲 |
| **Amazon Textract**                      | Structured CV parsing: skills, experience, education from PDF/DOCX                                               | 🔲 |
| **ASYNC EVENT PROCESSING**               |                                                                                                                  |        |
| **Amazon SQS + DLQ**                     | Code grading queue + video frame analysis queue; Dead Letter Queue on all queues for reliability                   | 🔲 |
| **AWS Step Functions**                   | 6-state Express Workflow: aggregate → score → narrative → store → notify                                          | 🔲 |
| **Amazon EventBridge**                   | Routes SessionEnded, CodeSubmitted, FrameCaptured events to appropriate workers                                   | 🔲 |
| **Amazon SNS**                           | Recruiter + candidate notifications: report ready, interview complete                                             | 🔲 |
| **DEVOPS & OBSERVABILITY**               |                                                                                                                  |        |
| **IAM Groups (ai / frontend / backend)** | Least-privilege groups for 5 team members; scoped to relevant services per role                                   | ✅ |
| **GitHub OIDC → IAM Role**               | No long-lived credentials; OIDC token → STS AssumeRoleWithWebIdentity; scoped to S3 sync + CF invalidation        | ✅ |
| **Terraform IaC (6 modules)**            | `networking` · `dns` · `auth` · `database` · `frontend` · `iam`; `terraform plan/apply`; state in `.tfstate`     | ✅ |
| **GitHub Actions CI/CD**                 | Frontend: build → S3 sync → CloudFront invalidation via OIDC role. Backend: build → ECR → ECS blue/green          | 🔲 |
| **CloudWatch + X-Ray**                   | RDS logs (postgresql/upgrade) exported to CloudWatch; full custom metrics + tracing planned for backend           | ✅/🔲 |
| **Security Hub + GuardDuty**             | CIS AWS Foundations compliance checks and threat detection                                                        | 🔲 |

## **2.3 Data Flow by Stage**

### **Stage 1: CV Parsing & Job Setup**

High-level:

- The monolith issues S3 presigned URLs, receives metadata, and triggers parsing workflows.
- AI enrichment (Textract + Bedrock) creates structured candidate profiles and JD context.

Step-by-step:

1. Candidate uploads CV in the frontend.
2. Frontend calls API Gateway -> ALB -> ECS Fargate monolith for a presigned URL.
3. CV uploads directly to S3; S3 event sends message to SQS.
4. Worker reads SQS -> Textract extracts text -> Bedrock classifies skills.
5. Structured profile saved to DynamoDB and indexed for the session.
6. Recruiter uploads JD to S3; Bedrock Knowledge Base ingestion builds RAG context.

### **Stage 2: Real-Time Voice Interview (Critical Path \< 1.5s)**

High-level:

- WebSocket traffic flows through API Gateway to the monolith for real-time orchestration.
- Transcribe -> Bedrock -> Polly generates the AI response and streams it back.

Step-by-step:

1. Candidate mic captures 250ms audio chunks.
2. WebSocket sends chunks to API Gateway -> ALB -> ECS monolith.
3. Monolith streams audio to Transcribe and receives partial transcripts.
4. On utterance end, monolith calls Bedrock with session context and live code state.
5. Bedrock response sent to Polly for TTS; audio streamed back over WebSocket.
6. Monaco editor sends code diffs via WebSocket; monolith stores current code state.

### **Stage 3: Async Analysis Pipeline (Parallel, Non-Blocking)**

High-level:

- Non-blocking analysis runs via SQS-triggered workers and sandboxed Fargate tasks.
- Results flow back into DynamoDB and are visible to the monolith and dashboard.

Step-by-step:

1. Frontend sends webcam frames to monolith; frames are queued in SQS.
2. Worker processes frames with Rekognition and writes emotion scores to DynamoDB.
3. Candidate submits code; monolith enqueues grading job to SQS.
4. ECS Fargate sandbox runs tests and returns results to DynamoDB.
5. Monolith reads results for follow-up questions and report generation.

### **Stage 4: Report Generation via Step Functions**

High-level:

- Step Functions aggregates session data and generates the report asynchronously.
- Notifications are sent after the report is stored and indexed.

Step-by-step:

1. Session ends; monolith emits SessionEnded event to EventBridge.
2. Step Functions aggregates DynamoDB records for the session.
3. Bedrock scores each Q&A pair in parallel.
4. Composite score is calculated and narrative is generated.
5. Report stored to S3 and summary saved to RDS.
6. SNS notifies recruiter and candidate.

## **2.4 Infrastructure Flow (Target — Full Architecture)**

> This describes the **complete target architecture** for the end of Sprint 4. For the **currently deployed IaC**, see Section 2.5 below.

High-level: Client → CloudFront → Cognito (direct SDK) → API Gateway → ALB → ECS Fargate monolith → data and AI services.

Step-by-step:

1. User loads the React SPA from CloudFront + S3 (WAF attached).
2. Browser authenticates **directly with Cognito SDK** (OAuth 2.0 PKCE). API Gateway validates the JWT on every subsequent call.
3. API Gateway uses VPC Link to ALB, which routes to ECS Fargate tasks in private subnet.
4. Monolith checks session in Redis, reads/writes RDS and DynamoDB, stores files in S3.
5. AI calls are made to Transcribe, Bedrock, Polly, and Rekognition.
6. Async tasks flow through SQS → EventBridge → Step Functions for code grading and reports.
7. CloudWatch metrics, logs, and X-Ray traces feed dashboards and alarms.
8. CI/CD builds, tests, and deploys with blue/green rollback on ECS. Terraform manages all resources.

---

## **2.5 AWS Infrastructure — Current IaC Flow (Junior-Friendly)**

> This section reflects **exactly what is deployed in Terraform right now**. Planned services for Sprint 2–4 are shown separately at the bottom.

![SmartHire AI — Current Deployed Infrastructure](./SmartHire_IaC_Current_Architecture.png)

---

### ✅ DEPLOYED LAYERS

---

### **Layer 1 — Frontend Delivery (CloudFront + S3 + Route 53)**

The user visits `smarthire-ai.org`. Here is exactly what happens:

- **Route 53** maps the domain to CloudFront via an A-record alias (also `www`).
- **CloudFront** serves the React SPA globally from edge locations. An **existing WAF** is attached, blocking SQL injection and XSS.
- **S3** stores the static files. Public access is **fully blocked** — only CloudFront can read S3 using **Origin Access Control (OAC + SigV4 signing)**.
- **ACM Certificate** (provisioned in `us-east-1`, required for CloudFront) enforces TLS 1.2+.
- 403/404 errors both redirect to `index.html` so React Router handles client-side navigation.

```
Browser  →  Route 53  →  CloudFront (WAF + OAC + TLS 1.2+)  →  S3 (private bucket)
```

---

### **Layer 2 — Authentication (Cognito — Direct Client SDK)**

> **Key design point:** The browser authenticates **directly with Cognito SDK** — no API Gateway in the auth path.

- The **Cognito App Client** uses OAuth 2.0 **authorization code flow with PKCE**, `generate_secret = false` (public client, browser-safe).
- Supported auth flows: `SRP_AUTH`, `USER_PASSWORD_AUTH`, `REFRESH_TOKEN_AUTH`.
- **Google SSO** federated via Cognito Identity Provider. Mapping: `email → email`, `sub → username`.
- Users have a **`custom:role`** attribute (`RECRUITER` or `CANDIDATE`) set at registration.
- After signin, Cognito returns **JWT tokens** (ID token + access token). The frontend attaches these to every API call.

```
Browser  →  Cognito (OAuth 2.0 code + PKCE)
          ↓ or via Google SSO
          → Google Identity Provider → Cognito (maps custom:role)
          → JWT tokens returned to browser
```

---

### **Layer 3 — User Sync: PostConfirmation Lambda → RDS**

When the user **confirms their email**, Cognito fires a **PostConfirmation trigger** to a Lambda.

**Exact flow (`cognito_post_confirmation`, Node.js 20):**

1. Lambda runs inside the **private DB subnet** (`10.0.10.0/24`, `10.0.11.0/24`) — no internet access.
2. To get credentials, it calls **Secrets Manager via a VPC Interface Endpoint** (no NAT Gateway required).
3. Secrets Manager (KMS-decrypted) returns: `host`, `port`, `dbname`, `username`, `password`.
4. Lambda connects to RDS and runs:

```sql
INSERT INTO Users (Id, CognitoSub, Email, Role, CreatedAt)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (Email) DO NOTHING
```

5. Connection closes. Lambda returns the event to Cognito. Cognito completes signup.

```
Cognito PostConfirmation
  →  Lambda (private subnet, app-sg, Node.js 20)
      →  VPC Endpoint → Secrets Manager (KMS decrypted credentials)
      →  RDS port 5432 (rds-sg: only allows app-sg ingress)
         INSERT INTO Users
```

**Why VPC Endpoint instead of NAT Gateway?** NAT costs ~$32/month fixed. VPC Endpoints cost ~$7/month. Lambda in a private subnet uses VPC Endpoints to reach AWS APIs with zero internet exposure.

---

### **Layer 4 — Database + Admin Access (RDS + Bastion)**

**RDS PostgreSQL 15 (private subnet):**
- `publicly_accessible = false`, encrypted with KMS, storage gp3.
- **Single-AZ** (cost-appropriate for Junior level; upgrade to Multi-AZ in production).
- Automatic backup: 7-day retention, window `03:00–04:00 UTC`.
- CloudWatch logs: `postgresql` + `upgrade` log streams.
- IAM DB authentication enabled (future: replace password with IAM auth).

**Bastion Host (public subnet, admin only):**
- EC2 `t3.micro`, Amazon Linux 2023, Elastic IP, SSH port 22 from admin CIDR only.
- PostgreSQL 15 client pre-installed. Used for: `ssh -L 5432:rds-endpoint:5432 ec2-user@bastion-eip` → run migrations.

**Security Group rules:**
```
app-sg     →  rds-sg : port 5432  (Lambda reaches RDS)
bastion-sg →  rds-sg : port 5432  (Admin SSH tunnel reaches RDS)
rds-sg : ALL other ingress blocked
```

---

### **Layer 5 — Security Foundation**

| Service | IaC Detail |
|---|---|
| **KMS Key** | Auto-rotation ON, 7-day deletion window, alias `smarthire-secrets-{env}` |
| **Secrets Manager** | Stores `host/port/user/password/dbname` for RDS, KMS encrypted |
| **IAM Groups** | `ai-group`, `frontend-group`, `backend-group` — least-privilege per team role |
| **GitHub OIDC** | No static AWS keys; OIDC token → STS `AssumeRoleWithWebIdentity` → temp creds |

---

### **Layer 6 — CI/CD (Frontend Deployed; Backend Planned)**

**Current frontend deploy flow:**
```
git push → GitHub Actions
  →  OIDC token → AWS STS → github-actions-frontend role (temp creds, 15 min TTL)
  →  npm run build
  →  aws s3 sync ./dist  →  S3 bucket
  →  aws cloudfront create-invalidation  →  cache cleared globally
```

**Why OIDC?** Zero secrets stored in GitHub. Token proves the workflow is running on your repo, not a stolen credential.

---

### 🔲 PLANNED LAYERS (Sprint 2–4)

![SmartHire AI — Planned Infrastructure Sprint 2–4](./SmartHire_Planned_Architecture.png)

**Sprint 2 — API + Compute + Sessions**
- API Gateway REST API: rate limiting, response caching, JWT authorizer (Cognito), routes `/jobs` `/sessions` `/reports` `/users`
- API Gateway WebSocket API: `$connect/$disconnect`, `sendAudio`, `sendCodeDiff`, `sendFrame` routes
- VPC Link → ALB → ECS Fargate (ASP.NET Monolith) in private app subnet
  - Modules: Auth (JWT/RBAC), Job Management, Session Controller, AI Orchestrator, Report Trigger
- ElastiCache Redis: session cache, rate-limit counters, WebSocket connectionIds
- DynamoDB: Sessions table (connectionId, state, questions) + Interviews table (Q&A, emotion scores, code state)

**Sprint 3 — AI + Async Pipeline**
- S3 Buckets: `smarthire-recordings` (SSE-KMS, Glacier 30d) + `smarthire-resumes-reports`
- Amazon SQS: `code-grading-queue` (+ DLQ) + `video-analysis-queue` (+ DLQ)
- ECS Fargate Code Executor: Node 20 / Python 3.11 / Java 21, 30s timeout, 512MB, triggered by SQS
- Amazon Rekognition: DetectFaces per frame → CALM/CONFUSED/FEAR + FacePose, triggered by SQS
- Amazon Textract → Bedrock Claude 3.5: CV parsing pipeline → structured skills JSON → DynamoDB
- Bedrock Knowledge Base (RAG): JD S3 upload → 512-token chunks → Titan Embeddings → vector index

**Sprint 4 — Voice AI + Events + Observability + Security**
- Voice pipeline (critical path < 1.5s): `Mic → WebSocket → Monolith → Transcribe (streaming) → Bedrock (streaming) → Polly → WebSocket → Browser`
- Amazon EventBridge: routes `SessionEnded → Step Functions`, `CodeSubmitted → SQS`, `FrameCaptured → SQS`
- AWS Step Functions (Express Workflow): Aggregate → Score (parallel) → Composite → Narrative → Store → SNS Notify
- CloudWatch: custom metrics (P95/P99, active sessions, Bedrock tokens), dashboards, alarms → SNS (< 5 min)
- AWS X-Ray: distributed tracing across all services
- Security Hub + GuardDuty + AWS Config: CIS Foundations Benchmark, threat detection, compliance rules
- Backend CI/CD: GitHub Actions → Docker build → Trivy scan → ECR → ECS Blue/Green → rollback < 5 min

---

### **Current Infrastructure — Full ASCII Flow**

```
┌──────────────────────────────────────────────────────────────────┐
│  BROWSER (React SPA)                                             │
└──────────┬───────────────────────────┬───────────────────────────┘
           │ Page load (HTTPS)         │ Login (OAuth 2.0 + PKCE)
           ▼                           ▼
┌────────────────────────┐   ┌─────────────────────────────────────┐
│ Route 53               │   │ Amazon Cognito User Pool            │
│ → CloudFront (WAF,OAC) │   │ Google SSO | Email+Password         │
│ → S3 (private, OAC)    │   │ custom:role = RECRUITER/CANDIDATE   │
│   TLS 1.2+, ACM cert   │   │ JWT returned to browser             │
└────────────────────────┘   └──────────────┬──────────────────────┘
                                            │ PostConfirmation trigger
                                            ▼
                             ┌──────────────────────────────────────┐
                             │ VPC 10.0.0.0/16                      │
                             │                                      │
                             │ Public Subnets (10.0.1-2.0/24)       │
                             │ ┌──────────────────────────────┐     │
                             │ │ Bastion EC2 t3.micro + EIP   │     │
                             │ │ bastion-sg: SSH from CIDR    │     │
                             │ │ → RDS port 5432 (admin only) │     │
                             │ └──────────────────────────────┘     │
                             │                                      │
                             │ Private DB Subnets (10.0.10-11/24)   │
                             │ ┌──────────────────────────────┐     │
                             │ │ Lambda cognito_post_confirm  │     │
                             │ │ Node.js 20 | app-sg          │     │
                             │ │ No internet — VPC Endpoints  │     │
                             │ │  ┌─ Secrets Manager Endpoint │     │
                             │ │  │  (KMS decrypted creds)    │     │
                             │ │  └─ KMS Endpoint             │     │
                             │ │ → RDS port 5432              │     │
                             │ │   INSERT INTO Users          │     │
                             │ └──────────────────────────────┘     │
                             │ ┌──────────────────────────────┐     │
                             │ │ RDS PostgreSQL 15             │     │
                             │ │ single-AZ | gp3 | KMS enc    │     │
                             │ │ backup 7d | CW logs enabled  │     │
                             │ │ rds-sg: only from app-sg     │     │
                             │ │         + bastion-sg         │     │
                             │ └──────────────────────────────┘     │
                             └──────────────────────────────────────┘

GitHub Actions
  → OIDC token → STS → github-actions-frontend (temp creds)
  → npm build → s3 sync + cloudfront invalidation

Terraform IaC (6 modules)
  networking | dns | auth | database | frontend | iam
  → terraform plan/apply → all resources above managed as code
```



# **3\. Detailed Role Assignments**

Each team member owns a complete vertical slice of the system. Responsibilities span from AWS infrastructure setup through business logic to frontend integration, ensuring clear accountability with no single point of failure on any major feature.

## **3.1 Backend 1 — System Architect & API Lead**

| BE1: System Architect & API Lead _"The foundation, security, and authentication layer of SmartHire"_ Complexity: 5 / 5 AWS Account & IAM: Roles and policies for all services, Cognito User Pool (Recruiter/Candidate/Admin roles), Google OAuth federation, JWT Authorizer on API Gateway REST endpoints Auth Pipeline: Direct Cognito Auth / Hosted UI via Frontend SDK. Lambda Post-Confirmation trigger đồng bộ tài khoản mới đăng ký từ Cognito vào RDS PostgreSQL RDS PostgreSQL Schema: Users, Companies, Jobs, Subscriptions, BillingEvents tables with Flyway migration scripts, RDS Proxy for Lambda connection pooling Job Management Microservice: CRUD for job postings, JD upload via S3 presigned URL, trigger Bedrock Knowledge Base re-sync on each new JD upload AWS SAM IaC (template.yaml): Defines API Gateway REST routes, Cognito User Pool / Clients, Lambda Post Confirmation, RDS instance, ElastiCache cluster, WAF web ACL with managed rule groups CI/CD Pipeline: GitHub Actions using AWS OIDC (no long-lived credentials); path-filtered triggers so /services/jobs/\*\* only deploys job-stack |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |

## **3.2 Backend 2 — WebSocket & Pipeline Engineer**

| BE2: WebSocket & Pipeline Engineer _"The real-time nervous system of the entire interview session"_ Complexity: 5 / 5 API Gateway WebSocket API: $connect, $disconnect, sendAudio, sendCodeDiff, sendFrame, getSessionStatus custom route handlers with connectionId stored to DynamoDB Sessions table Interview Session Microservice: POST /sessions (create), PUT /sessions/:id/start, PUT /sessions/:id/end; session lifecycle state machine; stores connectionId per active connection AI Orchestrator Lambda: Audio chunk buffering \--\> utterance silence detection \--\> Transcribe Streaming \--\> Bedrock (streaming response mode) \--\> Polly \--\> postToConnection WebSocket push pipeline ECS Fargate Code Executor: Dockerfiles per language (Node 20 / Python 3.11 / Java 21); Lambda triggers ECS RunTask API; CPU/memory limits enforced; 30-second hard timeout; test results written to DynamoDB Step Functions Workflow: 6-state Express Workflow with retry policies on all states; parallel scoring state invokes Bedrock concurrently for multiple Q\&A pairs to reduce total report time SQS and EventBridge Wiring: S3 frames to SQS (video analysis), code submissions to SQS (grading), session end to EventBridge to Step Functions; Dead Letter Queues on all SQS queues |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |

## **3.3 Frontend 1 — Dashboard & DevOps Lead**

| FE1: Dashboard & DevOps Lead _"The insights layer \- where raw data becomes actionable hiring decisions"_ Complexity: 4 / 5 Infrastructure: S3 bucket for React hosting, CloudFront distribution with OAC, Route 53 A-record alias, CloudFront cache invalidation on every deploy via GitHub Actions React Foundation: TypeScript \+ Tailwind CSS \+ TanStack Query (API state management) \+ Zustand (auth store, session store) \+ React Router v6 with protected route guards Recruiter Dashboard: Job create/edit form, JD file upload with progress indicator, candidate invite modal, active sessions list with live status polling, scheduled interviews calendar Report Dashboard: Recharts radar chart (5 skill dimensions), emotion timeline line chart with color-coded zones, per-question score breakdown table, S3 presigned video replay player with emotion timestamp markers CloudWatch Monitoring Board: Custom dashboard showing Lambda invocation counts, P99 latency per service, active WebSocket connections, Bedrock token usage, Lambda error rates; CloudWatch Alarms configured Admin Panel: User and company management, subscription tier status, usage analytics (interviews per month per company), estimated cost per session display |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |

## **3.4 Frontend 2 — Interview Room & Media Engineer**

| FE2: Interview Room & Media Engineer _"The candidate-facing experience \- the most visible and impressive part of the platform"_ Complexity: 5 / 5 Interview Room Layout: Three-panel split view \- AI status panel (waveform \+ live transcript) | Monaco code editor (full-screen toggle) | candidate webcam feed with live emotion badge WebSocket Audio Pipeline: getUserMedia() microphone capture \--\> MediaRecorder (audio/webm, 250ms timeslice) \--\> FileReader readAsDataURL \--\> strip data URI prefix \--\> WebSocket.send() binary data AI Audio Playback: Receive Base64 MP3 chunks from WebSocket \--\> atob() decode \--\> ArrayBuffer \--\> AudioContext.decodeAudioData \--\> AudioBufferSourceNode.start() \--\> seamless playback queue Monaco Editor Integration: npm monaco-editor, language selector (JavaScript/Python/Java/C\#), code diff sync via WebSocket on onChange (debounced 500ms) so AI always sees current code state Video Frame Pipeline: setInterval every 5 seconds \--\> Canvas.drawImage(videoElement) \--\> toDataURL("image/jpeg", 0.7) \--\> POST /frames \--\> parse response emotion score \--\> update live gauge widget Session State Machine: idle \--\> permission-check \--\> connecting \--\> in-session (recording | AI-thinking | AI-speaking) \--\> ended; clear visual indicators for each state; auto-reconnect on WebSocket drop |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | -------------------------------------------------------------------------------------------------- |

## **3.5 AI Engineer — Brain & Intelligence Layer**

| AI Engineer: Brain & Intelligence Layer _"The differentiator that makes SmartHire fundamentally better than every competitor"_ Complexity: 4.5 / 5 Bedrock RAG Pipeline: Create Knowledge Base in Bedrock; configure S3 data source with JD files chunked by section (Skills / Requirements / Responsibilities, 512-token chunks); Amazon Titan Embeddings; sync triggered on each new JD upload Interviewer Prompt Engineering: System prompt with three persona modes (Friendly / Standard / Strict), role context injected from JD RAG retrieval, candidate profile summary from CV parse, conversation history window (last 10 turns with token budget guard), live code state injection ("Current candidate code: \[code_state\]") CV Parsing Pipeline: Textract extracts raw text \--\> Bedrock prompt categorizes into {frontend_skills, backend_skills, devops_skills, soft_skills, years_experience, seniority_estimate} \--\> validated JSON stored to DynamoDB candidate-profiles table Emotion Analysis: Per-frame Rekognition.DetectFaces call \--\> extract top-3 emotions with confidence scores \+ FacePose (Yaw/Pitch as proxy for eye contact direction) \--\> time-stamped records in DynamoDB \--\> aggregation for report: % time calm, average attention score, peak stress moments STAR Scoring Rubric: Per-answer Bedrock evaluation prompt returning strict JSON {technical_accuracy: 0-10, depth_of_reasoning: 0-10, communication_clarity: 0-10, star_structure: 0-10, justification: "brief explanation"} \--\> validated and stored per question in DynamoDB Latency Optimization: Transcribe partial results with 200ms silence threshold for early LLM triggering; Bedrock streaming response (push first token to Polly immediately); SSML prosody marks in Polly for natural pacing; end-to-end profiling with X-Ray until P95 \< 1.5s confirmed |
| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |

# **4\. 4-Week Sprint Plan**

Agile model: weekly sprint cycles with Friday demo checkpoints. Daily 15-minute async standups via Slack (Done / Doing / Blockers). Shared AWS account with separate IAM roles per team member. Feature branches with PR review before merging to main, which triggers CI/CD auto-deploy.

| WEEK 1 \- Foundation & Environment Setup Goal: Demo Friday: Working login flow, candidate dashboard visible, all members deployed something to AWS.                            |                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **BE1**                                                                                                                                                                        | IAM roles and policies for all team members, Cognito User Pool with 3 app clients, Lambda Post-Confirmation trigger đê sync vào DB, RDS PostgreSQL with Flyway migrations, SAM template.yaml skeleton, GitHub Actions CI/CD with AWS OIDC integration                                                     |
| **BE2**                                                                                                                                                                        | API Gateway WebSocket API created, $connect and $disconnect Lambda handlers deployed, DynamoDB Sessions table with GSI design, end-to-end WebSocket test with wscat tool to confirm connectivity                                                                                                          |
| **FE1**                                                                                                                                                                        | React \+ TypeScript \+ Tailwind \+ TanStack Query \+ Zustand project scaffolded, S3 bucket \+ CloudFront distribution live, Route 53 domain configured, Login and Register pages wired to Cognito auth flow                                                                                               |
| **FE2**                                                                                                                                                                        | Interview Room component shell created, Monaco Editor npm package integrated with syntax highlighting for 4 languages, getUserMedia() microphone permission flow working, basic Web Audio API recording test                                                                                              |
| **AI**                                                                                                                                                                         | Bedrock API credentials verified and working, Claude 3.5 Sonnet baseline prompt test (question generation from sample JD), Amazon Transcribe batch test with sample audio file, Polly voice synthesis test, Knowledge Base created with 2 sample JDs indexed                                              |
| **WEEK 2 \- Core Voice Interview Pipeline** Goal: Demo Friday: Speak into mic, AI transcribes, AI responds with voice. The defining milestone of the project.                  |                                                                                                                                                                                                                                                                                                           |
| **BE1**                                                                                                                                                                        | Job Management Lambda CRUD endpoints deployed, JD S3 presigned URL endpoint live, Bedrock Knowledge Base sync triggered on new JD upload, all API Gateway REST routes documented in Swagger                                                                                                               |
| **BE2**                                                                                                                                                                        | Audio chunk WebSocket handler receiving binary data, Transcribe Streaming integration working with real-time transcript output, AI Orchestrator Lambda wiring complete (Transcribe to Bedrock to Polly to WebSocket push), full voice roundtrip latency measured and below 1.5s                           |
| **FE1**                                                                                                                                                                        | Recruiter dashboard complete: job creation form, JD upload with file progress bar, candidate invitation modal with email input, navigation shell with role-based sidebar, all TanStack Query hooks for existing APIs                                                                                      |
| **FE2**                                                                                                                                                                        | MediaRecorder 250ms chunk loop sending to WebSocket, received AI audio playing through AudioContext without gaps, interview room state machine implemented with visual indicators for recording/thinking/speaking states                                                                                  |
| **AI**                                                                                                                                                                         | Interviewer persona prompts for all 3 modes tested and tuned, RAG-augmented question generation validated against 5 different JDs, conversation history management with token budget guard implemented and tested                                                                                         |
| **WEEK 3 \- Advanced Features: Code \+ Emotion Analysis** Goal: Demo Friday: Full mock interview with a live coding question \+ emotion timeline chart displayed in real time. |                                                                                                                                                                                                                                                                                                           |
| **BE1**                                                                                                                                                                        | CV parsing pipeline end-to-end working (S3 trigger to Textract to Bedrock to DynamoDB), Free/Pro subscription tiers in RDS, WAF rules live and tested (SQLi/XSS blocked), KMS encryption enabled on Recordings S3 bucket, all secrets moved to Secrets Manager                                            |
| **BE2**                                                                                                                                                                        | ECS Fargate Docker containers built for Node/Python/Java, pushed to ECR, Lambda ECS RunTask integration working, SQS code grading queue processing results, Rekognition frame analysis Lambda worker live, EventBridge session-end rule triggering Step Functions                                         |
| **FE1**                                                                                                                                                                        | Report dashboard complete with Recharts radar chart, emotion timeline chart, score breakdown table, video replay player with presigned URL, CloudWatch custom dashboard built, X-Ray active tracing enabled on all Lambdas                                                                                |
| **FE2**                                                                                                                                                                        | Canvas frame capture sending to /frames every 5 seconds, Monaco code diff syncing to backend via WebSocket on every change, live emotion gauge widget updating in interview room, code submission button with test results display panel                                                                  |
| **AI**                                                                                                                                                                         | Code-aware follow-up prompts injecting live code state into Bedrock context and tested, STAR scoring rubric with structured JSON output validated against 20 sample answers, Rekognition emotion aggregation algorithm producing meaningful scores, answer quality tester script built                    |
| **WEEK 4 \- Polish, Security & Demo Preparation** Goal: Demo Day Ready: system is stress-tested, secure, and two full scripted demo scenarios are rehearsed and ready.         |                                                                                                                                                                                                                                                                                                           |
| **BE1**                                                                                                                                                                        | Full security audit: all API keys in Secrets Manager, zero hardcoded credentials, GDPR delete endpoint removing user data from RDS/S3/DynamoDB/Cognito, RDS Proxy live for connection pooling, load test JWT Authorizer và Cognito Login flow                                                             |
| **BE2**                                                                                                                                                                        | Latency profiling with X-Ray: trace every hop in voice pipeline, identify and fix bottlenecks, tune Transcribe silence threshold to 200ms, Bedrock streaming response enabled, stress test 20 concurrent sessions running simultaneously without degradation                                              |
| **FE1**                                                                                                                                                                        | CloudWatch Alarms configured (Lambda error rate \> 1% and P99 latency \> 2s send SNS alert), admin panel feature complete, cost-per-session tracking display in dashboard, all team-reported UI bugs fixed, final integration test of all features together                                               |
| **FE2**                                                                                                                                                                        | WebSocket auto-reconnect with session resume (connectionId stored in DynamoDB), mobile-responsive interview room layout, loading skeleton screens for all data fetching, React error boundaries preventing full page crashes, basic accessibility review (keyboard navigation, contrast ratios)           |
| **AI**                                                                                                                                                                         | Final prompt calibration: reduce hallucinations on technical topics, improve follow-up question depth and relevance, scoring calibration against human expert baseline on 10 sample sessions, full Demo Persona A script (strong: 85/100) and Demo Persona B script (weak: 42/100) prepared and rehearsed |

# **5\. Requirements Matrix & Risk Management**

| REQUIREMENT                 | AWS SERVICES                             | DIFF    | MAIN RISK                                | MITIGATION                                                                                             |
| :-------------------------- | :--------------------------------------- | ------- | :--------------------------------------- | :----------------------------------------------------------------------------------------------------- |
| **Serverless Architecture** | ECS Fargate, API GW, DynamoDB, S3        | **5/5** | Container cold start latency             | Fargate tasks with autoscaling and warm pools during peak hours                                        |
| **WebSocket Real-Time**     | API Gateway WebSocket API                | **5/5** | Connection drops mid-interview           | Client auto-reconnect with exponential backoff; session state persisted in DynamoDB, not in-memory     |
| **Voice Pipeline \<1.5s**   | Transcribe \+ Bedrock \+ Polly           | **5/5** | Bedrock latency variance                 | Streaming mode for Bedrock; Transcribe partial results for early trigger; Polly pre-buffer audio start |
| **Monolithic Backend**      | Single ECS service + unified data access | **4/5** | Large codebase complexity                | Clear module boundaries, shared domain model, strict API versioning and linting                        |
| **Async Pipeline**          | SQS, EventBridge, Step Functions         | **4/5** | Message loss on Lambda timeout           | DLQ on all SQS queues; Step Functions retry with exponential backoff; idempotency keys on all workers  |
| **RAG Personalization**     | Bedrock Knowledge Base                   | **4/5** | Low retrieval relevance                  | Chunk JD by section at 512 tokens; test retrieval accuracy with 10 sample queries before demo day      |
| **Code Sandbox**            | ECS Fargate containers                   | **4/5** | Container cold start latency             | Pre-warm container pool; 30s hard timeout enforced; 512MB memory limit per sandbox task                |
| **Security Stack**          | KMS, WAF, Cognito, Secrets Manager       | **4/5** | JWT validation inconsistency             | Shared .NET NuGet middleware for JWT validation; used by all Lambda services from a single library     |
| **Infrastructure as Code**  | Terraform + ECS blue/green               | **4/5** | Config drift between environments        | Terraform workspaces, locked state, and automated rollback on failed deploys                           |
| **CI/CD Pipeline**          | GitHub Actions + ECR + ECS               | **4/5** | Failed deploy in production              | Build/test/scan gates, blue/green with health checks, automatic rollback                               |
| **Observability**           | CloudWatch + X-Ray + custom metrics      | **4/5** | Hidden latency spikes                    | Custom metrics, logs, traces, and alarms on P95/P99 and error rate                                     |
| **Security (CIS)**          | Security Hub + Config + GuardDuty        | **4/5** | Misconfigured resources                  | CIS checks, continuous compliance, least privilege IAM, mandatory encryption                           |
| **API Gateway Controls**    | Throttling + caching + WAF               | **4/5** | Abuse or cost spikes                     | Usage plans, rate limits, cache TTL, WAF managed rules                                                 |
| **Backup & Restore**        | RDS PITR + DynamoDB PITR + S3 versioning | **4/5** | Data loss or accidental deletion         | Automated backups, cross-region copies, and tested restore runbooks                                    |
| **Multi-AZ Database**       | RDS PostgreSQL Multi-AZ \+ RDS Proxy     | **3/5** | Connection exhaustion under Lambda scale | RDS Proxy handles Lambda connection pooling; read replica dedicated to Report Service queries          |

# **6\. Non-Functional Requirements (NFR)**

These metrics are the graduation criteria for the project. All NFRs must be demonstrably measured and met during Week 4 stress testing before the final presentation to the AWS Director.

| CATEGORY          | METRIC                        | TARGET                                   | IMPLEMENTATION                                                                                             |
| :---------------- | :---------------------------- | :--------------------------------------- | :--------------------------------------------------------------------------------------------------------- |
| **Latency**       | Voice-to-Voice roundtrip      | **P95 \< 1.5 seconds**                   | Transcribe streaming partial results \+ Bedrock streaming \+ Polly neural \+ audio pre-buffering on client |
| **Latency**       | REST API endpoints            | **P95 \< 300ms**                         | ECS autoscaling, RDS Proxy, DynamoDB single-digit ms reads, Redis caching                                  |
| **Latency**       | Report generation             | **\< 90 seconds after session end**      | Step Functions parallel scoring states; Bedrock invoked concurrently for multiple Q\&A pairs               |
| **Scalability**   | Concurrent interview sessions | **50+ simultaneous**                     | Lambda auto-scales per request; DynamoDB on-demand capacity; API Gateway 10k req/s default limit           |
| **Scalability**   | Recording storage growth      | **Unlimited (pay-per-GB)**               | S3 Intelligent-Tiering; Glacier lifecycle transition after 30 days; no capacity planning needed            |
| **Availability**  | Public API uptime             | **99.9% SLA**                            | Multi-AZ RDS automatic failover; DynamoDB 99.999% SLA; API Gateway multi-AZ by default                     |
| **Availability**  | Interview session continuity  | **Zero data loss on network drop**       | WebSocket connectionId \+ full session state in DynamoDB; client reconnects and resumes seamlessly         |
| **Security**      | Video and audio recordings    | **AES-256 at rest, TLS 1.2+ in transit** | S3 SSE-KMS with automatic key rotation; API Gateway enforces minimum TLS 1.2; WAF on all endpoints         |
| **Security**      | User data deletion rights     | **GDPR-compliant within 30 days**        | DELETE /users/:id triggers Step Functions: removes records from RDS, S3 objects, DynamoDB, Cognito         |
| **Cost**          | Cost per 45-minute session    | **Target \< $0.10**                      | Bedrock token budget guard per session; Transcribe streaming pricing model; Lambda duration optimization   |
| **Cost**          | Idle infrastructure cost      | **Approximately $0 when no sessions**    | ECS Fargate serverless tasks with scale-to-zero pattern and no always-on servers                           |
| **Observability** | Error detection and alerting  | **\< 5 minutes to alert on anomaly**     | CloudWatch alarms on P95/P99 latency and error rate; SNS sends email and Slack alert                       |
| **Security**      | CIS compliance posture        | **Continuous**                           | Security Hub + Config rules mapped to CIS AWS Foundations Benchmark                                        |
| **CI/CD**         | Deployment reliability        | **Rollback \< 5 minutes**                | ECS blue/green with automatic rollback on failed health checks                                             |
| **Backup**        | Recovery targets              | **RPO 15 min, RTO 60 min**               | RDS PITR, DynamoDB PITR, S3 versioning and cross-region replication                                        |

| Demo Day Scenario — End of Week 4 Two scripted candidate personas that showcase AI intelligence in action:Persona A (Strong Candidate): Provides correct, well-structured answers and maintains confident eye contact. AI escalates to harder follow-up questions and probes code choices. Final scorecard: 85/100 with highlighted technical strengths.Persona B (Weak Candidate): Gives vague answers, looks away from camera frequently. AI adapts by simplifying questions and calling out knowledge gaps directly. Final scorecard: 42/100 with a specific 3-point improvement plan.Showcase items: side-by-side scorecard comparison, emotion timeline charts for both sessions, recruiter comparison view, live latency measurement displayed on screen (target: speech to AI response in under 1.5 seconds), and cost dashboard confirming $0.07 total for a 45-minute session. |
