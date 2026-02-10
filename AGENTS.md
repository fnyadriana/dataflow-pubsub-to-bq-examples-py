# Agent Guidelines for Dataflow Pub/Sub to BigQuery Pipeline

This document provides comprehensive instructions and guidelines for AI agents and developers working on this repository.

## 1. Project Overview

This repository contains Apache Beam pipelines (Python) for reading from Google Cloud Pub/Sub and writing to BigQuery. Three pipeline variants are provided:

1. **Standard (Flattened)**: Parses JSON fields into specific BigQuery columns.
2. **Raw JSON**: Ingests the full payload into a BigQuery `JSON` column.
3. **Schema-Driven**: Fetches Avro schema from the Pub/Sub Schema Registry at startup and dynamically generates BigQuery table schema and field extraction logic.

**Key Technologies:** Python 3.12+, `uv` package manager, Apache Beam 2.70.0 (GCP extras).

## 2. Environment Setup & Build

**Manager:** `uv`
-   **Sync Environment:**
    ```bash
    uv sync
    ```
-   **Add Dependency:**
    ```bash
    uv add <package_name>
    ```
-   **Build Wheel (for Dataflow workers):**
    ```bash
    uv build --wheel
    ```

## 3. Testing & Verification

**Tool:** `pytest`
-   **Run Tests:** `uv run pytest`

## 4. Linting & Formatting

**Tool:** `ruff`
-   **Lint:** `uv run ruff check .`
-   **Format:** `uv run ruff format .`

## 5. Code Style & Conventions

-   **Indentation:** 4 spaces.
-   **Type Hints:** Mandatory for all function signatures.
-   **Docstrings:** Google Style (`Args:`, `Returns:`, `Raises:`).
-   **Naming:** `snake_case` for functions/vars, `CamelCase` for classes.
-   **Imports:** Grouped (StdLib, ThirdParty, Local), sorted alphabetically.

## 6. Repository Structure

```
dataflow-pubsub-to-bq-examples-py/
├── dataflow_pubsub_to_bq/          # Main source code
│   ├── pipeline.py                 # Entry point (Standard/Flattened)
│   ├── pipeline_json.py            # Entry point (Raw JSON)
│   ├── pipeline_schema_driven.py   # Entry point (Schema-Driven)
│   ├── pipeline_options.py         # Options (Standard + JSON)
│   ├── pipeline_schema_driven_options.py  # Options (Schema-Driven)
│   └── transforms/                 # Custom Beam PTransforms
│       ├── json_to_tablerow.py     # Flattened JSON parsing
│       ├── raw_json.py             # Raw JSON parsing
│       └── schema_driven_to_tablerow.py  # Schema-driven Avro-based parsing
├── schemas/                        # Avro schema definitions
│   ├── taxi_ride_v1.avsc           # v1 schema for Pub/Sub Schema Registry
│   └── taxi_ride_v2.avsc           # v2 schema (adds enrichment fields)
├── scripts/                        # Supporting scripts
│   ├── cleanup_schema_driven.sh    # Teardown all schema-driven resources
│   ├── enrich_taxi_ride.yaml       # SMT definition for v2 enrichment
│   ├── generate_bq_schema.py       # BQ schema generator from registry
│   ├── publish_to_schema_topic.py  # Mirror publisher (pass-through relay)
│   ├── run_mirror_publisher.sh     # Mirror publisher launcher
│   └── run_schema_evolution.sh     # Phase 2: schema evolution + v2 publisher
├── tests/                          # Unit tests
│   ├── test_json_to_tablerow.py
│   ├── test_raw_json.py
│   └── test_schema_driven_to_tablerow.py
├── docs/                           # Design documents
│   └── schema_evolution_plan.md    # Schema evolution design doc
├── run_dataflow.sh                 # Deployment script (Standard)
├── run_dataflow_json.sh            # Deployment script (JSON)
├── run_dataflow_schema_driven.sh   # Deployment script (Schema-Driven)
└── pyproject.toml                  # Project configuration and dependencies
```

## 7. Workflow for Agents

1.  **Identify Pipeline:** Determine which pipeline variant the task targets (standard, JSON, or schema-driven).
2.  **Contextualize:** Read `pyproject.toml` and the relevant `run_*.sh` script to understand how the code is built and deployed.
3.  **Develop:** Edit in `dataflow_pubsub_to_bq/`. Use type hints for all function signatures.
4.  **Verify:** Lint (`uv run ruff check .`) and test (`uv run pytest`).
5.  **Finalize:** Ensure all tests pass and debug artifacts are removed.
