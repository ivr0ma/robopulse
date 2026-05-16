"""
PySpark Gold job: Silver-партиции в S3 -> Gold-партиция признаков.

Job строит один дневной срез признаков по каждому роботу. Airflow передает
только нужные партиции Silver, чтобы Spark не перечитывал всю историю.

Запуск:
    spark-submit build_robot_reliability_features.py \
        --task-reports-paths s3a://robopulse/silver/task_reports/dt=2026-04-29 \
        --maintenance-events-paths s3a://robopulse/silver/maintenance_events/dt=2026-04-29 \
        --robot-status-paths s3a://robopulse/silver/robot_status/dt=2026-04-29 \
        --gold-path s3a://robopulse/gold \
        --partition-dt 2026-04-30
"""

import argparse
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from spark.common.spark_session import build_spark


TASK_REPORTS_SCHEMA = T.StructType(
    [
        T.StructField("report_id", T.StringType()),
        T.StructField("robot_serial_number", T.StringType()),
        T.StructField("robot_display_name", T.StringType()),
        T.StructField("operator", T.StringType()),
        T.StructField("map_name", T.StringType()),
        T.StructField("cleaning_mode", T.StringType()),
        T.StructField("task_end_status", T.IntegerType()),
        T.StructField("completion_percentage", T.DoubleType()),
        T.StructField("duration_seconds", T.IntegerType()),
        T.StructField("planned_area_m2", T.DoubleType()),
        T.StructField("actual_area_m2", T.DoubleType()),
        T.StructField("efficiency_m2_per_hour", T.DoubleType()),
        T.StructField("water_consumption_l", T.DoubleType()),
        T.StructField("start_battery_pct", T.IntegerType()),
        T.StructField("end_battery_pct", T.IntegerType()),
        T.StructField("battery_delta_pct", T.IntegerType()),
        T.StructField("brush_residual_pct", T.IntegerType()),
        T.StructField("filter_residual_pct", T.IntegerType()),
        T.StructField("suction_blade_residual_pct", T.IntegerType()),
        T.StructField("start_time", T.StringType()),
        T.StructField("end_time", T.StringType()),
        T.StructField("dt", T.DateType()),
    ]
)

MAINTENANCE_EVENTS_SCHEMA = T.StructType(
    [
        T.StructField("maintenance_id", T.StringType()),
        T.StructField("robot_id", T.StringType()),
        T.StructField("service_ticket_id", T.StringType()),
        T.StructField("service_source", T.StringType()),
        T.StructField("maintenance_type", T.StringType()),
        T.StructField("maintenance_category", T.StringType()),
        T.StructField("maintenance_status", T.StringType()),
        T.StructField("reported_at", T.StringType()),
        T.StructField("started_at", T.StringType()),
        T.StructField("completed_at", T.StringType()),
        T.StructField("severity", T.StringType()),
        T.StructField("failure_detected", T.BooleanType()),
        T.StructField("failure_code", T.StringType()),
        T.StructField("component", T.StringType()),
        T.StructField("action_taken", T.StringType()),
        T.StructField("parts_replaced", T.ArrayType(T.StringType())),
        T.StructField("downtime_minutes", T.IntegerType()),
        T.StructField("technician_id", T.StringType()),
        T.StructField("comment", T.StringType()),
        T.StructField("created_at", T.StringType()),
        T.StructField("updated_at", T.StringType()),
        T.StructField("dt", T.DateType()),
    ]
)

ROBOT_STATUS_SCHEMA = T.StructType(
    [
        T.StructField("serial_number", T.StringType()),
        T.StructField("poll_ts", T.StringType()),
        T.StructField("task_state", T.StringType()),
        T.StructField("online", T.BooleanType()),
        T.StructField("speed_kmh", T.DoubleType()),
        T.StructField("battery_soc", T.IntegerType()),
        T.StructField("battery_charging", T.BooleanType()),
        T.StructField("battery_cycle_times", T.IntegerType()),
        T.StructField("battery_health", T.StringType()),
        T.StructField("battery_temp1", T.IntegerType()),
        T.StructField("battery_temp2", T.IntegerType()),
        T.StructField("total_voltage_mv", T.IntegerType()),
        T.StructField("localization_state", T.StringType()),
        T.StructField("map_id", T.StringType()),
        T.StructField("map_name", T.StringType()),
        T.StructField("pos_x", T.IntegerType()),
        T.StructField("pos_y", T.IntegerType()),
        T.StructField("nav_status", T.StringType()),
        T.StructField("clean_water_tank_pct", T.IntegerType()),
        T.StructField("recovery_water_tank_pct", T.IntegerType()),
        T.StructField("rolling_brush_used_life_h", T.DoubleType()),
        T.StructField("rolling_brush_lifespan_h", T.IntegerType()),
        T.StructField("right_side_brush_used_life_h", T.DoubleType()),
        T.StructField("soft_squeegee_used_life_h", T.DoubleType()),
        T.StructField("clean_water_filter_used_life_h", T.DoubleType()),
        T.StructField("hepa_sensor_used_life_h", T.DoubleType()),
        T.StructField("dt", T.DateType()),
    ]
)


def split_paths(paths_arg: str) -> list[str]:
    """
    Разбивает CLI-аргумент со списком путей на отдельные значения.

    Args:
        paths_arg: Строка путей, разделенных запятыми.

    Returns:
        Непустой список путей без лишних пробелов.
    """
    return [path.strip() for path in paths_arg.split(",") if path.strip()]


def infer_base_path(paths: list[str]) -> str | None:
    """
    Определяет корневой путь таблицы по первому partition-path.

    Args:
        paths: Список путей к партициям Spark table.

    Returns:
        Корневой путь таблицы или None, если путь не содержит dt-партицию.
    """
    if not paths or "/dt=" not in paths[0]:
        return None

    return paths[0].split("/dt=", maxsplit=1)[0]


def resolve_existing_paths(
    spark: SparkSession,
    paths_arg: str,
) -> list[str]:
    """
    Оставляет только существующие пути или glob-шаблоны.

    Args:
        spark: Активная SparkSession.
        paths_arg: Один путь или список путей через запятую.

    Returns:
        Список путей, которые Spark сможет прочитать.
    """
    existing_paths = []
    hadoop_conf = spark._jsc.hadoopConfiguration()

    for raw_path in split_paths(paths_arg):
        path = spark._jvm.org.apache.hadoop.fs.Path(raw_path)
        fs = path.getFileSystem(hadoop_conf)
        statuses = fs.globStatus(path)
        if statuses is not None and len(statuses) > 0:
            existing_paths.append(raw_path)

    return existing_paths


def read_parquet_or_empty(
    spark: SparkSession,
    paths_arg: str,
    schema: T.StructType,
    dataset_name: str,
) -> DataFrame:
    """
    Читает существующие Parquet-партиции или возвращает пустой DataFrame.

    Args:
        spark: Активная SparkSession.
        paths_arg: Один путь или список путей через запятую.
        schema: Схема пустого DataFrame на случай отсутствия партиций.
        dataset_name: Имя датасета для логов.

    Returns:
        DataFrame с данными из переданных партиций.
    """
    existing_paths = resolve_existing_paths(spark, paths_arg)
    if not existing_paths:
        print(f"{dataset_name}: нет существующих партиций")
        return spark.createDataFrame([], schema)

    reader = spark.read
    base_path = infer_base_path(existing_paths)
    if base_path:
        reader = reader.option("basePath", base_path)

    return reader.parquet(*existing_paths)


def build_gold(
    spark: SparkSession,
    task_reports_paths: str,
    maintenance_events_paths: str,
    robot_status_paths: str,
    gold_path: str,
    partition_dt: str,
) -> int:
    """
    Строит Gold-партицию признаков за указанную дату.

    Args:
        spark: Активная SparkSession.
        task_reports_paths: Silver-партиции task_reports для окна 90 дней.
        maintenance_events_paths: Silver-партиции maintenance_events.
        robot_status_paths: Silver-партиции robot_status для окна 7 дней.
        gold_path: Корневой путь Gold-слоя.
        partition_dt: Дата формируемой Gold-партиции.

    Returns:
        Количество записанных строк Gold-витрины.
    """
    feature_date = datetime.strptime(partition_dt, "%Y-%m-%d").date()

    task_df = read_parquet_or_empty(
        spark=spark,
        paths_arg=task_reports_paths,
        schema=TASK_REPORTS_SCHEMA,
        dataset_name="task_reports",
    )
    maint_df = read_parquet_or_empty(
        spark=spark,
        paths_arg=maintenance_events_paths,
        schema=MAINTENANCE_EVENTS_SCHEMA,
        dataset_name="maintenance_events",
    )
    status_df = read_parquet_or_empty(
        spark=spark,
        paths_arg=robot_status_paths,
        schema=ROBOT_STATUS_SCHEMA,
        dataset_name="robot_status",
    )

    task_df.createOrReplaceTempView("task_reports")
    maint_df.createOrReplaceTempView("maintenance_events")

    last_w = (
        Window.partitionBy("serial_number", "dt")
        .orderBy("poll_ts")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    status_daily = (
        status_df
        .withColumn("_ct_last", F.last("battery_cycle_times").over(last_w))
        .withColumn("_bh_last", F.last("battery_health").over(last_w))
        .groupBy("serial_number", "dt")
        .agg(
            F.avg("battery_soc").alias("avg_battery_soc"),
            F.last("_ct_last").alias("battery_cycle_times"),
            F.last("_bh_last").alias("battery_health"),
        )
    )
    status_daily.createOrReplaceTempView("status_daily")

    gold_df = spark.sql(f"""
        WITH feature_dates AS (
            SELECT DISTINCT
                robot_serial_number AS serial_number,
                DATE '{feature_date.isoformat()}' AS feature_date
            FROM task_reports
            WHERE CAST(start_time AS DATE)
                  >= date_sub(DATE '{feature_date.isoformat()}', 90)
              AND CAST(start_time AS DATE) <= DATE '{feature_date.isoformat()}'
        ),

        task_feat AS (
            SELECT
                fd.serial_number,
                fd.feature_date,

                MAX(STRUCT(
                    CAST(t.start_time AS TIMESTAMP),
                    t.brush_residual_pct
                )).brush_residual_pct AS brush_residual_last,
                MAX(STRUCT(
                    CAST(t.start_time AS TIMESTAMP),
                    t.filter_residual_pct
                )).filter_residual_pct AS filter_residual_last,
                MAX(STRUCT(
                    CAST(t.start_time AS TIMESTAMP),
                    t.suction_blade_residual_pct
                )).suction_blade_residual_pct AS squeegee_residual_last,

                COUNT(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 7)
                    THEN 1
                END) AS missions_7d,
                COUNT(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 30)
                    THEN 1
                END) AS missions_30d,

                COALESCE(SUM(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 30)
                    THEN t.actual_area_m2
                END), 0) AS area_30d,
                COALESCE(SUM(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 90)
                    THEN t.actual_area_m2
                END), 0) AS area_90d,

                AVG(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 7)
                    THEN t.completion_percentage
                END) AS avg_completion_7d,
                AVG(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 30)
                    THEN t.completion_percentage
                END) AS avg_completion_30d,
                AVG(CASE
                    WHEN CAST(t.start_time AS DATE)
                         >= date_sub(fd.feature_date, 30)
                    THEN t.battery_delta_pct
                END) AS avg_battery_drop_30d

            FROM feature_dates fd
            LEFT JOIN task_reports t
                ON t.robot_serial_number = fd.serial_number
               AND CAST(t.start_time AS DATE) <= fd.feature_date
               AND CAST(t.start_time AS DATE)
                   >= date_sub(fd.feature_date, 90)
            GROUP BY fd.serial_number, fd.feature_date
        ),

        maint_feat AS (
            SELECT
                fd.serial_number,
                fd.feature_date,

                COALESCE(datediff(
                    fd.feature_date,
                    MAX(CASE
                        WHEN CAST(m.completed_at AS DATE) <= fd.feature_date
                        THEN CAST(m.completed_at AS DATE)
                    END)
                ), 999) AS days_since_last_maintenance,

                COALESCE(datediff(
                    fd.feature_date,
                    MAX(CASE
                        WHEN CAST(m.completed_at AS DATE) <= fd.feature_date
                         AND m.failure_detected = TRUE
                        THEN CAST(m.completed_at AS DATE)
                    END)
                ), 999) AS days_since_last_failure,

                COUNT(CASE
                    WHEN CAST(m.completed_at AS DATE)
                         >= date_sub(fd.feature_date, 30)
                     AND CAST(m.completed_at AS DATE) <= fd.feature_date
                    THEN 1
                END) AS maint_count_30d,

                COUNT(CASE
                    WHEN CAST(m.completed_at AS DATE)
                         >= date_sub(fd.feature_date, 90)
                     AND CAST(m.completed_at AS DATE) <= fd.feature_date
                    THEN 1
                END) AS maint_count_90d,

                COALESCE(SUM(CASE
                    WHEN CAST(m.completed_at AS DATE)
                         >= date_sub(fd.feature_date, 30)
                     AND CAST(m.completed_at AS DATE) <= fd.feature_date
                    THEN m.downtime_minutes
                END), 0) AS downtime_30d,

                COUNT(CASE
                    WHEN m.failure_detected = TRUE
                     AND CAST(m.completed_at AS DATE)
                         >= date_sub(fd.feature_date, 90)
                     AND CAST(m.completed_at AS DATE) <= fd.feature_date
                    THEN 1
                END) AS failure_count_90d,

                COUNT(CASE
                    WHEN m.maintenance_type = 'corrective'
                     AND CAST(m.completed_at AS DATE)
                         >= date_sub(fd.feature_date, 90)
                     AND CAST(m.completed_at AS DATE) <= fd.feature_date
                    THEN 1
                END) AS corrective_count_90d

            FROM feature_dates fd
            LEFT JOIN maintenance_events m ON m.robot_id = fd.serial_number
            GROUP BY fd.serial_number, fd.feature_date
        ),

        status_feat AS (
            SELECT
                fd.serial_number,
                fd.feature_date,
                MAX(STRUCT(
                    sd.dt,
                    sd.battery_cycle_times
                )).battery_cycle_times AS battery_cycle_times_last,
                MAX(STRUCT(
                    sd.dt,
                    sd.battery_health
                )).battery_health AS battery_health_last,
                AVG(sd.avg_battery_soc) AS avg_battery_soc_7d
            FROM feature_dates fd
            LEFT JOIN status_daily sd
                ON sd.serial_number = fd.serial_number
               AND sd.dt <= fd.feature_date
               AND sd.dt >= date_sub(fd.feature_date, 7)
            GROUP BY fd.serial_number, fd.feature_date
        ),

        target AS (
            SELECT
                fd.serial_number,
                fd.feature_date,
                COALESCE(MAX(CASE
                    WHEN m.failure_detected = TRUE THEN 1 ELSE 0
                END), 0) AS has_failure_next_30d
            FROM feature_dates fd
            LEFT JOIN maintenance_events m
                ON m.robot_id = fd.serial_number
               AND CAST(m.completed_at AS DATE) > fd.feature_date
               AND CAST(m.completed_at AS DATE)
                   <= date_add(fd.feature_date, 30)
            GROUP BY fd.serial_number, fd.feature_date
        )

        SELECT
            tf.serial_number,
            tf.feature_date,
            tf.brush_residual_last,
            tf.filter_residual_last,
            tf.squeegee_residual_last,
            tf.missions_7d,
            tf.missions_30d,
            tf.area_30d,
            tf.area_90d,
            tf.avg_completion_7d,
            tf.avg_completion_30d,
            tf.avg_battery_drop_30d,
            mf.days_since_last_maintenance,
            mf.days_since_last_failure,
            mf.maint_count_30d,
            mf.maint_count_90d,
            mf.downtime_30d,
            mf.failure_count_90d,
            mf.corrective_count_90d,
            sf.battery_cycle_times_last,
            sf.battery_health_last,
            sf.avg_battery_soc_7d,
            t.has_failure_next_30d,
            DATE '{feature_date.isoformat()}' AS dt
        FROM task_feat tf
        LEFT JOIN maint_feat mf
            ON mf.serial_number = tf.serial_number
           AND mf.feature_date = tf.feature_date
        LEFT JOIN status_feat sf
            ON sf.serial_number = tf.serial_number
           AND sf.feature_date = tf.feature_date
        LEFT JOIN target t
            ON t.serial_number = tf.serial_number
           AND t.feature_date = tf.feature_date
        ORDER BY tf.serial_number, tf.feature_date
    """)

    count = gold_df.count()
    if count == 0:
        print("Gold dataset: 0 rows")
        return 0

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    gold_df.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(
        f"{gold_path}/robot_features"
    )

    print(f"Gold dataset: {count} rows x {len(gold_df.columns)} columns")
    return count


def parse_args() -> argparse.Namespace:
    """
    Разбирает CLI-аргументы Gold job.

    Returns:
        Namespace с путями Silver-партиций и параметрами Gold-слоя.
    """
    parser = argparse.ArgumentParser(
        description="Build Robopulse Gold partition",
    )
    parser.add_argument("--task-reports-paths", required=True)
    parser.add_argument("--maintenance-events-paths", required=True)
    parser.add_argument("--robot-status-paths", required=True)
    parser.add_argument("--gold-path", required=True)
    parser.add_argument("--partition-dt", required=True)
    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint для spark-submit.
    """
    args = parse_args()
    spark = build_spark()

    print(f"Partition          : {args.partition_dt}")
    print(f"Task reports       : {args.task_reports_paths}")
    print(f"Maintenance events : {args.maintenance_events_paths}")
    print(f"Robot status       : {args.robot_status_paths}")
    print(f"Gold               : {args.gold_path}")

    try:
        build_gold(
            spark=spark,
            task_reports_paths=args.task_reports_paths,
            maintenance_events_paths=args.maintenance_events_paths,
            robot_status_paths=args.robot_status_paths,
            gold_path=args.gold_path,
            partition_dt=args.partition_dt,
        )
        print("=== Gold complete ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
