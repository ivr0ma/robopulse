import shutil
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import (
    DEFAULT_ARGS,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PARTITION_DT,
)


def _delete_s3_prefix(s3_client, prefix: str) -> None:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(Bucket=MINIO_BUCKET, Delete={"Objects": objects})


def _generate_and_upload(partition_dt: str) -> None:
    import boto3
    import subprocess

    tmp_root = Path("/tmp") / "robopulse_bronze_gen" / partition_dt
    if tmp_root.exists():
        shutil.rmtree(tmp_root)

    subprocess.run(
        [
            sys.executable,
            "/opt/airflow/generate_data.py",
            "--start-date", partition_dt,
            "--end-date", partition_dt,
            "--out", str(tmp_root),
            "--seed", "42",
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
        f"bronze/maintenance_events/source=synthetic/load_dt={partition_dt}/",
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

    print(f"Uploaded {uploaded} files for {partition_dt} to s3://{MINIO_BUCKET}/bronze/")


with DAG(
    dag_id="robopulse_source",
    description="Формирование Bronze-партиции источников Robopulse",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["robopulse", "source"],
) as dag:
    generate_and_upload_bronze = PythonOperator(
        task_id="generate_and_upload_bronze_partition",
        python_callable=_generate_and_upload,
        op_kwargs={"partition_dt": PARTITION_DT},
    )
