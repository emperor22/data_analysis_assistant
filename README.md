# Data Analysis Assistant

[![](https://github.com/emperor22/data_analysis_assistant/actions/workflows/main.yml/badge.svg)](https://github.com/emperor22/data_analysis_assistant/actions/workflows/main.yml)

### Deterministic LLM-Driven Data Analytics Platform

Data Analysis Assistant generates analytical workflows from dataset metadata, validates them against strict schemas, and executes them through a constrained operation set.

LLM output is treated as **untrusted planning data**. Validation and execution are separated to reduce hallucinations, prevent arbitrary code execution, and keep analysis runs reproducible.

## Live Demo

Demo: [LINK](https://daa-demo.xyz/)
Sample datasets: [LINK](https://drive.google.com/drive/folders/1KezB3ABQDVa-mlVQ3sGW-GD97bg68rUp?usp=sharing)

## Workflow Preview

<video src="https://github.com/user-attachments/assets/496e1d8d-cca2-49e9-ac22-f5492da2a083" width="800" height="400" controls></video>

1. Upload a dataset
2. Normalize and profile the data
3. Generate a structured analysis workflow using an LLM
4. Validate and filter generated tasks
5. Review or edit tasks in the UI
6. Execute validated tasks asynchronously
7. Render results as tables, charts, Excel files, or ZIP reports
8. Re-run tasks on the same or compatible datasets

## Key Features

### Dataset Normalization

Raw files are normalized before any LLM interaction.

* Format-agnostic ingestion with size and header validation
* Canonical column naming and datatype coercion
* Boolean, null, and datetime standardization
* Time-granularity inference
* Column-level statistical profiling
* Dataset fingerprinting for run isolation

The LLM receives **structured metadata**, not raw CSV text.

### LLM Planning & Validation

LLM planning runs through a controlled prompt pipeline.

* Two-stage prompt flow: dataset analysis → task synthesis
* Metadata-only context injection
* Strict Pydantic validation
* Invalid task filtering with request-level logging
* Dataset blacklisting after repeated invalid responses
* Rate-limit aware retries with bounded backoff
* Persisted request state transitions

Workflows must contain enough valid tasks to proceed. Invalid steps are removed rather than blindly executed.

### Safe Analysis Execution

All analysis steps execute through predefined JSON schemas and registered functions.

Supported operations include:

`groupby` · `filter` · `get_top_or_bottom_N_entries` · `get_proportion` · `get_column_statistics` · `resample_data` · `map` · `map_range` · `date_op` · `math_op`

Execution safeguards include:

* No arbitrary code execution
* Function-to-model dispatch
* Column existence checks
* Expression-column token matching
* Step-level failure isolation
* Output size thresholds
* Automatic table, line chart, or bar chart rendering
* Structured result persistence and version tracking

## Architecture

```text
Client
  ↓
FastAPI
(Auth, rate limit, request validation)
  ↓
Dataset Normalization & Profiling
  ↓
Celery IO Workers
(LLM planning)
  ↓
Structured Validation & Filtering
  ↓
Celery CPU Workers
(validated analysis execution)
  ↓
Postgres + Artifact Storage + Optional Email Dispatch
```

## Operational Controls

* OTP-based authentication
* Protected API endpoints
* Segmented Celery queues
* Controlled worker concurrency
* Endpoint rate limiting
* Redis-backed access tracking
* Artifact lifecycle cleanup
* Structured request logging
* Slow-execution alerts
* BYOK support with credential isolation
* Sentry exception monitoring

## Technology Stack

**Backend:** FastAPI, Celery, Redis, Postgres, SQLAlchemy, Pandas, DuckDB, Pydantic
**Frontend:** Streamlit
**Infrastructure:** Docker Compose, ARM64 builds, Nginx, GitHub Actions, Sentry

## Testing

The project uses validation-first tests across API, worker, and persistence layers.

```bash
pytest tests
```
