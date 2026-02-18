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
- `workflow_diagram.mmd`: Mermaid source for workflow diagram
- `Workflow Diagram.png`: exported workflow diagram image
- `capstone_deliverables.md`: detailed Markdown deliverables doc
- `capstone_deliverables_word.txt`: Word-ready final report text
- `task.md`: assignment requirements (source of truth)

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

2. Written Documentation:
- `capstone_deliverables_word.txt`
- `capstone_deliverables.md`

3. Proof of Execution:
- Use screenshots/video of prompt handoff (Chat AI -> IDE AI), final files, and test outputs.

## Notes
- This submission is intentionally based on two AI tools (Chat + IDE), which satisfies the requirement of using at least two AI UX types.
- `task.md` was used as the final deliverable and rubric reference.
