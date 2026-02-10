# Agent Guidelines for Dataflow Pub/Sub to BigQuery Pipeline

Instructions for AI agents working in this repository.

## Project Overview

Apache Beam pipelines (Python 3.12) for streaming Pub/Sub to BigQuery. Three variants:

1. **Standard ETL** (`pipeline.py`) -- parses JSON into typed BQ columns.
2. **Raw JSON ELT** (`pipeline_json.py`) -- ingests raw payload into a BQ `JSON` column.
3. **Schema-Driven ETL** (`pipeline_schema_driven.py`) -- fetches Avro schema from Pub/Sub Schema Registry at startup, dynamically generates BQ schema and field extraction.

**Stack:** Python 3.12, `uv` package manager, Apache Beam 2.70.0, `ruff`, `pytest`.

## Build and Test Commands

```bash
# Install / sync dependencies
uv sync

# Run all tests (20 tests)
uv run pytest

# Run a single test file
uv run pytest tests/test_schema_driven_to_tablerow.py

# Run a single test by name
uv run pytest tests/test_schema_driven_to_tablerow.py::test_avro_to_bq_schema_v1

# Run tests matching a keyword pattern
uv run pytest -k "v2"

# Lint (must pass with zero errors)
uv run ruff check .

# Auto-fix lint errors
uv run ruff check . --fix

# Format check (verify only)
uv run ruff format --check .

# Format (apply changes)
uv run ruff format .

# Build wheel (for Dataflow workers)
uv build --wheel

# Add a dependency
uv add <package_name>
```

Always run `uv run ruff check .` and `uv run pytest` before committing.

## Repository Structure

```
dataflow_pubsub_to_bq/           # Main source code
  pipeline.py                     # Entry point: Standard ETL
  pipeline_json.py                # Entry point: Raw JSON ELT
  pipeline_schema_driven.py       # Entry point: Schema-Driven ETL
  pipeline_options.py             # Options: Standard + JSON
  pipeline_schema_driven_options.py  # Options: Schema-Driven
  transforms/                     # Custom Beam DoFns
    json_to_tablerow.py           # Standard: field-by-field parsing + DLQ
    raw_json.py                   # JSON: raw payload storage + DLQ
    schema_driven_to_tablerow.py  # Schema-driven: Avro-based dynamic extraction
tests/                            # Unit tests (pytest, no fixtures/mocks)
schemas/                          # Avro .avsc files (input for registry upload)
scripts/                          # Shell + Python support scripts
docs/                             # Design documents
```

## Code Style

### Formatting and Linting

- **Formatter/Linter:** `ruff` with default settings (no config overrides).
- **Indentation:** 4 spaces.
- **Line length:** 88 (ruff default). Long strings in argparse help or BQ schemas may exceed this.

### Imports

Three groups separated by blank lines: stdlib, third-party, local. Alphabetically sorted within each group. One import per line for single names; parenthesized multi-line for 2+ names from the same module:

```python
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import pubsub

from dataflow_pubsub_to_bq.transforms.schema_driven_to_tablerow import (
    ParseSchemaDrivenMessage,
    avro_to_bq_schema,
    get_envelope_bigquery_schema,
)
```

### Type Hints

Mandatory for all function signatures. Use modern Python 3.12+ syntax exclusively:

- `dict[str, Any]` not `Dict[str, Any]`
- `list[str]` not `List[str]`
- `str | None` not `Optional[str]`
- Only import `Any` from `typing` -- everything else uses builtins.
- DoFn `process()` returns `-> Any`.
- Pipeline `run(argv=None)` functions omit the return annotation.

### Naming Conventions

- **Functions/variables:** `snake_case`
- **Classes:** `PascalCase`
- **Module-private constants:** `_UPPER_SNAKE_CASE` (leading underscore), placed after imports
- **DoFn classes:** `PascalCase` describing the transformation (e.g., `ParseSchemaDrivenMessage`)
- **Beam step labels:** `"PascalCaseVerbPhrase"` strings (e.g., `"ReadFromPubSub"`, `"WriteToBigQuery"`)

### Docstrings

Google-style with `Args:`, `Returns:`, `Yields:`, `Raises:` sections. Required for all modules, classes, and public functions. DoFn `process()` methods use `Yields:` instead of `Returns:`.

```python
def avro_to_bq_schema(avro_schema_json: str) -> tuple[list[dict[str, str]], list[str], set[str]]:
    """Converts an Avro schema JSON string to BigQuery schema fields.

    Args:
        avro_schema_json: JSON string of the Avro schema definition.

    Returns:
        A tuple of (bq_fields, field_names, timestamp_fields) where:
        - bq_fields: List of BigQuery schema field dicts.
        - field_names: List of field name strings.
        - timestamp_fields: Set of field names that are TIMESTAMP type.

    Raises:
        ValueError: If the Avro schema contains unsupported types.
    """
```

### Error Handling

- **Standard/JSON pipelines:** Broad `try/except Exception` in `DoFn.process()`, route failures to DLQ via `beam.pvalue.TaggedOutput("dlq", error_row)` with error message + stack trace.
- **Schema-driven pipeline:** No try/except in `process()` -- schema registry validates at publish time, so no DLQ is needed.
- **Validation errors:** Raise `ValueError` with descriptive f-string messages.
- **Scripts:** Log errors with `logger.error(...)` and handle gracefully.

### Logging

- **Pipeline/transform files:** Use `logging.info("msg: %s", var)` directly (no module logger).
- **Script files:** Use `logger = logging.getLogger(__name__)` with `%s` lazy formatting.
- Prefer `%s` formatting over f-strings in log calls.

### Apache Beam Patterns

- DoFns are always classes inheriting `beam.DoFn`, never standalone functions.
- Pipeline options extend `PipelineOptions` with `@classmethod _add_argparse_args(cls, parser)`.
- BQ schemas are functions returning `list` of dicts: `[{"name": ..., "type": ..., "mode": "NULLABLE"}]`.
- Entry point is `run(argv=None)` with `if __name__ == "__main__":` guard.

## Testing Conventions

- **Framework:** `pytest` (no unittest classes).
- **Naming:** `test_<action>_<scenario>` as standalone functions, never in classes.
- **Assertions:** Bare `assert` statements; `pytest.raises(ValueError, match="...")` for exceptions.
- **Beam assertions:** `assert_that(pcollection, matcher, label="LabelName")` -- always provide `label`.
- **No mocks:** Tests use `TestPipeline` with `beam.Create([...])` for in-memory data.
- **Test data:** Module-level constants for reusable schemas; inline dicts for per-test payloads.
- **Docstrings:** Every test function has a one-line docstring describing what it verifies.

## Shell Script Conventions

- Shebang: `#!/bin/bash` with `set -e`.
- Variables: `UPPER_SNAKE_CASE`, always quoted as `"${VAR}"`.
- Conditionals: `[[ ]]` for string tests.
- Structure: Numbered steps with check-before-create idempotency pattern.
- Use `your-project-id` as the placeholder for project IDs.

## Agent Workflow

1. **Identify** which pipeline variant the task targets.
2. **Read** `pyproject.toml` and the relevant `run_*.sh` script for context.
3. **Develop** in `dataflow_pubsub_to_bq/`. Use type hints for all signatures.
4. **Verify** with `uv run ruff check .` and `uv run pytest`.
5. **Finalize** -- all tests must pass, lint must be clean, no debug artifacts.
