from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from common import BRONZE_S3, DEFAULT_ARGS, JARS, PARTITION_DT, SILVER_S3, SPARK_CONF


with DAG(
    dag_id="robopulse_silver",
    description="Bronze-партиция -> Silver-партиция",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["robopulse", "silver"],
) as dag:
    process_silver = SparkSubmitOperator(
        task_id="process_silver_partition",
        application="/opt/airflow/spark/jobs/silver/normalize_robot_operational_data.py",
        conn_id="spark_default",
        jars=JARS,
        conf=SPARK_CONF,
        application_args=[
            "--maintenance-events-path",
            f"{BRONZE_S3}/maintenance_events/source=synthetic/load_dt={PARTITION_DT}/*.json",
            "--task-reports-path",
            f"{BRONZE_S3}/task_reports/source=gausium/load_dt={PARTITION_DT}/*.json",
            "--robot-status-path",
            f"{BRONZE_S3}/robot_status/source=gausium/load_dt={PARTITION_DT}/*.jsonl",
            "--silver-path",
            SILVER_S3,
            "--partition-dt",
            PARTITION_DT,
        ],
        name="robopulse-silver",
        verbose=True,
    )
