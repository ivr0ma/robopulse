from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from common import (
    DEFAULT_ARGS,
    GOLD_S3,
    JARS,
    PARTITION_DT,
    SPARK_CONF,
    partition_paths,
    silver_paths_template,
)


with DAG(
    dag_id="robopulse_gold",
    description="Silver-партиции -> Gold-партиция признаков",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["robopulse", "gold"],
    user_defined_macros={"partition_paths": partition_paths},
) as dag:
    process_gold = SparkSubmitOperator(
        task_id="process_gold_partition",
        application="/opt/airflow/spark/jobs/gold/build_robot_reliability_features.py",
        conn_id="spark_default",
        jars=JARS,
        conf=SPARK_CONF,
        application_args=[
            "--task-reports-paths",
            silver_paths_template("task_reports", 90, 0),
            "--maintenance-events-paths",
            silver_paths_template("maintenance_events", 90, 30),
            "--robot-status-paths",
            silver_paths_template("robot_status", 7, 0),
            "--gold-path",
            GOLD_S3,
            "--partition-dt",
            PARTITION_DT,
        ],
        name="robopulse-gold",
        verbose=True,
    )
