# dags/modern_pipeline_dag.py
"""
KaremX Orders ETL Airflow DAG.

Daily orchestration pipeline that:
- Executes Spark ETL job for order data processing
- Sends Slack notifications on pipeline failures
- Runs daily with no catchup
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.task.trigger_rule import TriggerRule
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# DAG Configuration Constants
DAG_ID: str = "karemx_orders_etl"
DAG_OWNER: str = "KaremX"
DAG_DESCRIPTION: str = "Daily ETL pipeline for order processing"
SCHEDULE_INTERVAL: str = "@daily"
START_DATE: datetime = datetime(2024, 1, 1)
CATCHUP: bool = False

# Task Configuration Constants
SPARK_JOB_PATH: str = "jobs/modern_spark_job.py"
SPARK_APP_NAME: str = "KaremX_OrderETL"
SPARK_CONN_ID: str = "spark_default"
SLACK_CONN_ID: str = "slack_webhook"

# Spark Resource Configuration
EXECUTOR_MEMORY: str = "4G"
DRIVER_MEMORY: str = "2G"
SPARK_CONF: dict[str, str] = {
    "spark.sql.shuffle.partitions": "200",
}

# Airflow Retry Configuration
RETRIES: int = 2
RETRY_DELAY_MINUTES: int = 5

# Default arguments for all tasks in DAG
default_args: dict = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
}

# Define the DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=DAG_DESCRIPTION,
    schedule=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=["etl", "orders", "spark"],
)

# Task 1: Execute Spark ETL job
spark_job = SparkSubmitOperator(
    task_id="run_spark_etl",
    application=SPARK_JOB_PATH,
    conn_id=SPARK_CONN_ID,
    executor_memory=EXECUTOR_MEMORY,
    driver_memory=DRIVER_MEMORY,
    conf=SPARK_CONF,
    dag=dag,
    doc="""
    Executes the Spark ETL job to process raw orders.
    Transforms, cleans, and generates daily revenue summaries.
    """,
)

# Task 2: Slack failure notification
slack_failure_alert = SlackWebhookOperator(
    task_id="slack_failure_alert",
    slack_webhook_conn_id=SLACK_CONN_ID,
    message="""
    :x: KaremX Orders ETL Failed
    *DAG*: {{ dag.dag_id }}
    *Task*: {{ task_instance.task_id }}
    *Execution Time*: {{ ts }}
    *Log URL*: {{ task_instance.log_url }}
    """,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
    doc="Sends Slack notification when Spark ETL job fails.",
)

# Define task dependencies
spark_job >> slack_failure_alert
