# Data Analysis Assistant

[![](https://github.com/emperor22/data_analysis_assistant/actions/workflows/main.yml/badge.svg)](https://github.com/emperor22/data_analysis_assistant/actions/workflows/main.yml)

### A Deterministic LLM-Driven Automated Data Analytics Platform

converts dataset metadata into schema-constrained analytical workflows, **validates them structurally**, and executes them through a closed deterministic DSL.

LLM output is treated as untrusted planning data. Validation and execution are strictly separated to ensure safety, reproducibility, and operational control.


## Live Demo

🔗 [LINK](https://161.118.227.185/)

Dataset collections you can try: [LINK](https://drive.google.com/drive/folders/1KezB3ABQDVa-mlVQ3sGW-GD97bg68rUp?usp=sharing)

## Workflow Lifecycle

<video src="https://github.com/user-attachments/assets/8e9bae26-976e-45dc-8a93-83822df95f90" width="800" height="400" controls></video>

1. Upload dataset
2. LLM generates a structured analytical workflow (JSON schema enforced)
3. Run is queued for asynchronous processing (status tracked via UI)
4. User reviews and optionally edits generated tasks
5. Deterministic execution engine processes tasks step-by-step
6. Results rendered as:

   * Table
   * Line or bar chart (auto-inferred)
   * Excel export when output exceeds thresholds
7. Optional ZIP archive (PDF summary + Excel outputs) delivered via email
8. Tasks may be modified and re-run on the same or a new dataset


## Dataset Normalization & Profiling

Raw files are normalized before any LLM interaction.

* Format-agnostic ingestion with size and header validation
* Canonical column naming and datatype coercion with success-rate thresholds
* Boolean harmonization and null standardization
* Datetime detection and automatic time-granularity inference
* Column-level statistical profiling (missing ratios, skewness, distribution summaries)
* Deterministic dataset fingerprinting for run isolation

The LLM receives structured metadata and never raw CSV text.


## Prompt Orchestration

LLM planning occurs in a controlled, stateful pipeline.

* Two-stage prompt architecture (dataset analysis → task synthesis)
* Context injection using validated dataset metadata
* Strict Pydantic validation of each prompt stage
* Dataset-level blacklisting after repeated invalid responses
* Rate-limit aware retry logic with bounded backoff
* Explicit request state transitions persisted in the database

No prompt result advances without validation and persistence.


## LLM Validation Engine

LLM output is filtered, not blindly trusted.

Invalid steps are removed and logged with request context. Tasks are discarded only if structural integrity falls below minimum thresholds.

* Strict schemas (`extra="forbid"`) block hallucinated fields
* Function → model dispatch restricts steps to a registered DSL
* Expression–column token matching prevents undeclared references
* Required dataset columns enforced before execution eligibility
* Integrity thresholds prevent degraded workflows from executing

This preserves valid analytical intent while maintaining deterministic guarantees.


## Analytical DSL

All analyses execute through a closed, deterministic domain-specific language. No dynamic code execution is permitted.

## Core Operations

`groupby` · `filter` · `get_top_or_bottom_N_entries` · `get_proportion` · `get_column_statistics` · `resample_data`

## Structured Transformations

`map` · `map_range` · `date_op` · `math_op` · controlled column combinations

Every step and transformation is schema-validated before dispatch.


## Execution Engine

Validated workflows are executed step-by-step against a persisted dataset snapshot.

* Deterministic function dispatch
* Column existence enforcement at runtime
* Step-level failure isolation
* Output size thresholds and export routing
* Automatic visualization inference (line / bar / table)
* Structured result persistence and version tracking

Execution never evaluates arbitrary code.


## Architecture
```
  Client

    ↓

FastAPI (Auth + Rate Limit + Request Validation)

    ↓

Dataset Normalization & Profiling

    ↓

Celery IO Workers (LLM Planning)

    ↓

Structured Validation & Filtering

    ↓

Celery CPU Workers (Deterministic DSL Execution)

    ↓
  
Postgres + Artifact Storage + Optional Email Dispatch
```

## Operational Controls

Production safeguards around access, isolation, and workload discipline.

* OTP-based authentication with protected endpoints
* Segmented worker queues with controlled concurrency
* Endpoint rate limiting
* Dataset-level blacklisting for datasets with persistent validation failure
* Redis-backed access tracking and artifact lifecycle cleanup
* Structured request logging and slow-execution alerts
* BYOK support with credential isolation


## Observability

Explicit runtime transparency and failure boundaries.

* Centralized exception monitoring (Sentry)
* Structured logging for easier error tracing
* Deterministic task state transitions
* Clear rejection paths for invalid workflows


## Technology Stack

**Backend**
FastAPI · Celery · Redis · Postgres · SQLAlchemy · Pandas · DuckDB · Pydantic

**Frontend**
Streamlit (workflow inspection and editing)

**Infrastructure**
Docker Compose · ARM64 builds · Nginx · GitHub Actions · Sentry


## Testing

Validation-first testing strategy across layers.

* Async API endpoints coverage
* Celery task validation (eager mode)
* Dependency overrides for isolation
* Temporary SQLite database for testing

Run:

```bash
pytest tests
```
