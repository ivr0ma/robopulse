import os
from datetime import datetime, timedelta


MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "***")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "***")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "robopulse")

BRONZE_S3 = f"s3a://{MINIO_BUCKET}/bronze"
SILVER_S3 = f"s3a://{MINIO_BUCKET}/silver"
GOLD_S3 = f"s3a://{MINIO_BUCKET}/gold"

PARTITION_DT = "{{ dag_run.conf.get('partition_dt', ds) }}"
PARTITION_EXPR = "dag_run.conf.get('partition_dt', ds)"

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
    from datetime import datetime, timedelta

    target_dt = datetime.strptime(partition_dt, "%Y-%m-%d").date()
    start_dt = target_dt - timedelta(days=int(days_back))
    end_dt = target_dt + timedelta(days=int(days_forward))

    if end_dt < start_dt:
        return ""

    paths = []
    current_dt = start_dt
    while current_dt <= end_dt:
        paths.append(f"{root_path}/{table_name}/dt={current_dt.isoformat()}")
        current_dt += timedelta(days=1)

    return ",".join(paths)


def silver_paths_template(table_name: str, days_back: int, days_forward: int = 0) -> str:
    return (
        "{{ partition_paths('"
        f"{SILVER_S3}', '{table_name}', {PARTITION_EXPR}, "
        f"{days_back}, {days_forward}) }}}}"
    )
