"""
DAG: robopulse_pipeline
Bronze → Silver → Gold на PySpark + MinIO.

Задачи:
  1. generate_and_upload_bronze  — генерирует синтетические данные и загружает в MinIO
  2. process_silver              — PySpark: Bronze → нормализованный Parquet
  3. process_gold                — PySpark: Silver → feature dataset для ML
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── Константы ─────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "***")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "***")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "robopulse")

BRONZE_S3 = f"s3a://{MINIO_BUCKET}/bronze"
SILVER_S3 = f"s3a://{MINIO_BUCKET}/silver"
GOLD_S3   = f"s3a://{MINIO_BUCKET}/gold"

JARS = (
    "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.367.jar"
)

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint":                MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":              MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key":              MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access":       "true",
    "spark.hadoop.fs.s3a.impl":                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled":  "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.executor.memory":                        "3g",
    "spark.driver.memory":                          "2g",
    "spark.sql.shuffle.partitions":                 "8",
}

# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "robopulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="robopulse_pipeline",
    description="Bronze → Silver → Gold (PySpark + MinIO)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # запуск вручную; поменяйте на "@daily" для расписания
    catchup=False,
    tags=["robopulse"],
) as dag:

    # ── 1. Генерация Bronze и загрузка в MinIO ─────────────────────────────────

    def _generate_and_upload() -> None:
        import subprocess
        import boto3
        from pathlib import Path

        tmp_root = "/tmp/robopulse_bronze_gen"

        # Генерируем данные локально
        subprocess.run(
            [sys.executable, "/opt/airflow/generate_data.py",
             "--days", "180", "--out", tmp_root, "--seed", "42"],
            check=True,
        )

        # Загружаем в MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        bronze_dir = Path(tmp_root) / "bronze"
        uploaded = 0
        for file_path in sorted(bronze_dir.rglob("*")):
            if not file_path.is_file():
                continue
            s3_key = "bronze/" + file_path.relative_to(bronze_dir).as_posix()
            s3.upload_file(str(file_path), MINIO_BUCKET, s3_key)
            uploaded += 1

        print(f"Uploaded {uploaded} files → s3://{MINIO_BUCKET}/bronze/")

    t_bronze = PythonOperator(
        task_id="generate_and_upload_bronze",
        python_callable=_generate_and_upload,
    )

    # ── 2. Silver ──────────────────────────────────────────────────────────────

    t_silver = SparkSubmitOperator(
        task_id="process_silver",
        application="/opt/airflow/spark_jobs/normalize_robot_operational_data.py",
        conn_id="spark_default",
        jars=JARS,
        conf=SPARK_CONF,
        application_args=[BRONZE_S3, SILVER_S3],
        name="robopulse-silver",
        verbose=True,
    )

    # ── 3. Gold ────────────────────────────────────────────────────────────────

    t_gold = SparkSubmitOperator(
        task_id="process_gold",
        application="/opt/airflow/spark_jobs/build_robot_reliability_features.py",
        conn_id="spark_default",
        jars=JARS,
        conf=SPARK_CONF,
        application_args=[SILVER_S3, GOLD_S3],
        name="robopulse-gold",
        verbose=True,
    )

    t_bronze >> t_silver >> t_gold
