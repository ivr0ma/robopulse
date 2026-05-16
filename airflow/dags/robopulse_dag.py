"""
DAG'и Robopulse для поэтапного запуска Bronze, Silver и Gold.

Файл объявляет четыре независимых DAG:
    1. robopulse_source - формирует Bronze-партицию источников.
    2. robopulse_silver - нормализует Bronze-партицию в Silver.
    3. robopulse_gold - строит Gold-партицию признаков.
    4. robopulse_pipeline - управляющий DAG, запускающий остальные.
"""

import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "***")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "***")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "robopulse")

BRONZE_S3 = f"s3a://{MINIO_BUCKET}/bronze"
SILVER_S3 = f"s3a://{MINIO_BUCKET}/silver"
GOLD_S3 = f"s3a://{MINIO_BUCKET}/gold"

PARTITION_DT = "{{ dag_run.conf.get('partition_dt', macros.ds_add(ds, -1)) }}"
PARTITION_EXPR = "dag_run.conf.get('partition_dt', macros.ds_add(ds, -1))"

JARS = (
    "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.367.jar"
)

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
    "spark.executor.memory": "3g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}

DEFAULT_ARGS = {
    "owner": "robopulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def partition_paths(
    root_path: str,
    table_name: str,
    partition_dt: str,
    days_back: int,
    days_forward: int = 0,
) -> str:
    """
    Формирует список S3-партиций для SparkSubmitOperator.

    Args:
        root_path: Корневой путь слоя данных.
        table_name: Имя таблицы внутри слоя.
        partition_dt: Целевая дата партиции в формате YYYY-MM-DD.
        days_back: Сколько дней назад включить в окно.
        days_forward: Сколько дней вперед включить в окно.

    Returns:
        Строка с путями партиций, разделенными запятыми.
    """
    target_dt = datetime.strptime(partition_dt, "%Y-%m-%d").date()
    start_dt = target_dt - timedelta(days=int(days_back))
    end_dt = target_dt + timedelta(days=int(days_forward))

    if end_dt < start_dt:
        return ""

    paths = []
    current_dt = start_dt
    while current_dt <= end_dt:
        paths.append(
            f"{root_path}/{table_name}/dt={current_dt.isoformat()}"
        )
        current_dt += timedelta(days=1)

    return ",".join(paths)


def silver_paths_template(
    table_name: str,
    days_back: int,
    days_forward: int = 0,
) -> str:
    """
    Возвращает Jinja-шаблон со списком Silver-партиций.

    Args:
        table_name: Имя Silver-таблицы.
        days_back: Размер окна назад от целевой даты.
        days_forward: Размер окна вперед от целевой даты.

    Returns:
        Jinja-шаблон для аргумента Spark job.
    """
    return (
        "{{ partition_paths('"
        f"{SILVER_S3}', '{table_name}', {PARTITION_EXPR}, "
        f"{days_back}, {days_forward}) }}}}"
    )


def _delete_s3_prefix(s3_client, prefix: str) -> None:
    """
    Удаляет все объекты MinIO/S3 внутри указанного префикса.

    Args:
        s3_client: Клиент boto3 для доступа к MinIO/S3.
        prefix: Префикс объектов внутри bucket Robopulse.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(
                Bucket=MINIO_BUCKET,
                Delete={"Objects": objects},
            )


def _generate_and_upload(partition_dt: str) -> None:
    """
    Генерирует и загружает Bronze-партицию за указанную дату.

    Args:
        partition_dt: Дата бизнес-партиции в формате YYYY-MM-DD.
    """
    import boto3
    import subprocess

    tmp_root = Path("/tmp") / "robopulse_bronze_gen" / partition_dt
    if tmp_root.exists():
        shutil.rmtree(tmp_root)

    subprocess.run(
        [
            sys.executable,
            "/opt/airflow/generate_data.py",
            "--start-date",
            partition_dt,
            "--end-date",
            partition_dt,
            "--out",
            str(tmp_root),
            "--seed",
            "42",
        ],
        check=True,
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    partition_prefixes = [
        f"bronze/list_robots/source=gausium/load_dt={partition_dt}/",
        f"bronze/robot_status/source=gausium/load_dt={partition_dt}/",
        f"bronze/task_reports/source=gausium/load_dt={partition_dt}/",
        (
            "bronze/maintenance_events/source=synthetic/"
            f"load_dt={partition_dt}/"
        ),
    ]
    for prefix in partition_prefixes:
        _delete_s3_prefix(s3_client, prefix)

    bronze_dir = tmp_root / "bronze"
    uploaded = 0
    for file_path in sorted(bronze_dir.rglob("*")):
        if not file_path.is_file():
            continue

        s3_key = "bronze/" + file_path.relative_to(bronze_dir).as_posix()
        s3_client.upload_file(str(file_path), MINIO_BUCKET, s3_key)
        uploaded += 1

    print(
        f"Uploaded {uploaded} files for {partition_dt} "
        f"to s3://{MINIO_BUCKET}/bronze/"
    )


with DAG(
    dag_id="robopulse_source",
    description="Формирование Bronze-партиции источников Robopulse",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["robopulse", "source"],
) as source_dag:
    generate_and_upload_bronze = PythonOperator(
        task_id="generate_and_upload_bronze_partition",
        python_callable=_generate_and_upload,
        op_kwargs={"partition_dt": PARTITION_DT},
    )


with DAG(
    dag_id="robopulse_silver",
    description="Bronze-партиция -> Silver-партиция",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["robopulse", "silver"],
) as silver_dag:
    process_silver = SparkSubmitOperator(
        task_id="process_silver_partition",
        application=(
            "/opt/airflow/spark/jobs/silver/"
            "normalize_robot_operational_data.py"
        ),
        conn_id="spark_default",
        jars=JARS,
        conf=SPARK_CONF,
        application_args=[
            "--maintenance-events-path",
            (
                f"{BRONZE_S3}/maintenance_events/source=synthetic/"
                f"load_dt={PARTITION_DT}/*.json"
            ),
            "--task-reports-path",
            (
                f"{BRONZE_S3}/task_reports/source=gausium/"
                f"load_dt={PARTITION_DT}/*.json"
            ),
            "--robot-status-path",
            (
                f"{BRONZE_S3}/robot_status/source=gausium/"
                f"load_dt={PARTITION_DT}/*.jsonl"
            ),
            "--silver-path",
            SILVER_S3,
            "--partition-dt",
            PARTITION_DT,
        ],
        name="robopulse-silver",
        verbose=True,
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
) as gold_dag:
    process_gold = SparkSubmitOperator(
        task_id="process_gold_partition",
        application=(
            "/opt/airflow/spark/jobs/gold/"
            "build_robot_reliability_features.py"
        ),
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


with DAG(
    dag_id="robopulse_pipeline",
    description="Управляющий DAG Robopulse: Source -> Silver -> Gold",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["robopulse", "orchestration"],
) as orchestration_dag:
    run_source = TriggerDagRunOperator(
        task_id="run_source_dag",
        trigger_dag_id="robopulse_source",
        conf={"partition_dt": PARTITION_DT},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_silver = TriggerDagRunOperator(
        task_id="run_silver_dag",
        trigger_dag_id="robopulse_silver",
        conf={"partition_dt": PARTITION_DT},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_gold = TriggerDagRunOperator(
        task_id="run_gold_dag",
        trigger_dag_id="robopulse_gold",
        conf={"partition_dt": PARTITION_DT},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_source >> run_silver >> run_gold
