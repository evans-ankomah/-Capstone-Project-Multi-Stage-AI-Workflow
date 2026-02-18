# AI Capstone: Pipeline Modernizer

## Overview
This project modernizes a legacy CSV cleanup workflow at KaremX using two AI UX tools:
- Chat AI (initial architecture/code generation)
- IDE AI (code refinement and hardening)

The legacy script is transformed into:
- A PySpark ETL job
- An Airflow DAG definition

## Project Structure
- `legacy/process_data.py`: original legacy data-cleaning script
- `data/raw_orders.csv`: sample input dataset
- `jobs/modern_spark_job.py`: modernized PySpark ETL job
- `jobs/modern_pipeline_dag.py`: Airflow DAG for orchestration


## Quick Start
### 1) Run legacy baseline
```bat
python legacy\process_data.py
```

Expected outputs:
- `data/clean_orders.csv`
- `data/daily_summary.csv`

### 2) Syntax check modern files
```bat
python -m py_compile jobs\modern_spark_job.py jobs\modern_pipeline_dag.py
```

## Capstone Deliverables
1. Workflow Diagram:
- `Workflow Diagram.png`

## Notes
- This submission is intentionally based on two AI tools (Chat + IDE), which satisfies the requirement of using at least two AI UX types.
- `task.md` was used as the final deliverable and rubric reference.
