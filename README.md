# Simple Data Pipeline

A demonstrator for a simple data pipeline.

## Overview

This project demonstrates a simple data pipeline with the following features:

- Import raw data in parquet format
- Convert data to target format
- Demonstrate various data processing techniques:
  - Data mapping transformations
  - Splitting datasets
  - Joining multiple datasets

## Setup

1. **Create virtual environment**
   ```sh
   uv venv .venv
   ```
2. **Install dependencies**
   ```sh
   uv pip install -e .
   ```
3. **Run the pipeline**
   ```sh
   .venv/bin/python -m app.run_pipeline
   ```
   
   The pipeline requires one of the following options:
   ```sh
   # Import data from source to raw format (Parquet)
   .venv/bin/python -m app.run_pipeline --raw
   
   # Convert raw data to target format
   .venv/bin/python -m app.run_pipeline --target
   ```
   
   Running without options will display a prompt to specify either `--raw` or `--target`.

## Tasks

You can use [Taskfile](https://taskfile.dev) to automate common tasks:

- `task venv`   : Create venv with uv
- `task install`: Install dependencies
- `task check`  : Run ruff linter
- `task format` : Format code using ruff
- `task import-to-raw`    : Convert JSON data from source to raw Parquet format
- `task convert-to-target` : Transform raw Parquet data to target format
- `task run-pipeline`     : Run the complete data pipeline (import and convert)
- `task clean`  : Clean build and cache files

## Linting

Only [ruff](https://github.com/astral-sh/ruff) is used for linting.

## Requirements
- Python >= 3.8
- [uv](https://github.com/astral-sh/uv)
- [Taskfile](https://taskfile.dev)
