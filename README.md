#  SmartHire AI Core

Welcome to the AI microservices repository for **SmartHire**, an automated, AI-driven technical interview platform. 

This repository contains the intelligence layer of the application, designed as stateless AWS Lambda functions. These services handle real-time interview orchestration, computer vision emotion tracking, and automated post-interview evaluation.

##  Architecture Overview

The AI pipeline is divided into four distinct stages, triggered by event-driven AWS infrastructure (SQS, API Gateway WebSockets, and Step Functions):

1. **Pre-Interview (CV Parsing):** Extracts candidate skills from uploaded PDFs and structures them into JSON profiles.
2. **Real-Time Interview (Orchestrator):** Conducts a dynamic technical interview. It analyzes the candidate's live code state and conversation history to generate context-aware questions, synthesizing the text into an ultra-low latency audio stream.
3. **Async Vision Analytics:** Processes webcam frames at regular intervals to track candidate emotion profiles and head pose metrics (attention tracking).
4. **Post-Interview Reporting:** A Step Functions pipeline that evaluates technical accuracy using the STAR method, aggregates vision metrics, and generates a definitive "Hire / No Hire" executive summary.

##  Repository Structure

The codebase is strictly separated into production deployment files and local testing scripts.

```text
├── production_lambdas/
│   ├── stage1_setup/
│   │   └── cv_parser.py                   # AWS Textract & Bedrock Profile Generator
│   ├── stage2_realtime/
│   │   └── ai_orchestrator_lambda.py      # Bedrock Claude 3.5 & Polly TTS Engine
│   ├── stage3_vision/
│   │   └── emotion_tracker_lambda.py      # Amazon Rekognition Worker
│   └── stage4_evaluation/
│       ├── star_evaluator_lambda.py       # STAR Method Grader
│       ├── emotion_aggregator_lambda.py   # Vision Metrics Cruncher
│       └── report_generator_lambda.py     # Final Executive Summary Generator
│
├── local_tests_and_scripts/
│   ├── simulate_interview.py              # CLI tool for testing prompt injection defenses
│   └── ...                                # Deprecated local testing scripts
└── README.md