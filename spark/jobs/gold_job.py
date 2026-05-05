"""
PySpark Gold job: Silver (Parquet в S3) → feature dataset для ML.

Строит еженедельные срезы признаков по каждому роботу.
Горизонт признаков: 90 дней назад от feature_date.
Горизонт цели: 30 дней вперёд.

Запуск:
    spark-submit gold_job.py s3a://robopulse/silver s3a://robopulse/gold
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("robopulse-gold").getOrCreate()


def build_gold(spark: SparkSession, silver: str, gold: str) -> int:
    task_df = spark.read.parquet(f"{silver}/task_reports")
    maint_df = spark.read.parquet(f"{silver}/maintenance_events")
    status_df = spark.read.parquet(f"{silver}/robot_status")

    task_df.createOrReplaceTempView("task_reports")
    maint_df.createOrReplaceTempView("maintenance_events")

    # Агрегируем статус до дня: берём последнее значение cycle_times и health за день
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

    gold_df = spark.sql("""
        WITH feature_dates AS (
            SELECT DISTINCT
                robot_serial_number                                          AS serial_number,
                CAST(date_trunc('WEEK', CAST(start_time AS DATE)) AS DATE)   AS feature_date
            FROM task_reports
            WHERE CAST(start_time AS DATE) BETWEEN DATE '2025-12-01' AND DATE '2026-04-01'
        ),

        task_feat AS (
            SELECT
                fd.serial_number,
                fd.feature_date,

                -- Последний известный остаток расходника: MAX(STRUCT(ts, val)).val
                -- Spark сравнивает STRUCT лексикографически → MAX даёт значение при MAX(ts)
                MAX(STRUCT(CAST(t.start_time AS TIMESTAMP), t.brush_residual_pct)).brush_residual_pct
                    AS brush_residual_last,
                MAX(STRUCT(CAST(t.start_time AS TIMESTAMP), t.filter_residual_pct)).filter_residual_pct
                    AS filter_residual_last,
                MAX(STRUCT(CAST(t.start_time AS TIMESTAMP), t.suction_blade_residual_pct)).suction_blade_residual_pct
                    AS squeegee_residual_last,

                COUNT(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 7)  THEN 1 END) AS missions_7d,
                COUNT(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 30) THEN 1 END) AS missions_30d,

                COALESCE(SUM(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 30)
                                  THEN t.actual_area_m2 END), 0) AS area_30d,
                COALESCE(SUM(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 90)
                                  THEN t.actual_area_m2 END), 0) AS area_90d,

                AVG(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 7)
                         THEN t.completion_percentage END) AS avg_completion_7d,
                AVG(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 30)
                         THEN t.completion_percentage END) AS avg_completion_30d,
                AVG(CASE WHEN CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 30)
                         THEN t.battery_delta_pct END)     AS avg_battery_drop_30d

            FROM feature_dates fd
            JOIN task_reports t
                ON  t.robot_serial_number = fd.serial_number
                AND CAST(t.start_time AS DATE) <  fd.feature_date
                AND CAST(t.start_time AS DATE) >= date_sub(fd.feature_date, 90)
            GROUP BY fd.serial_number, fd.feature_date
        ),

        maint_feat AS (
            SELECT
                fd.serial_number,
                fd.feature_date,

                COALESCE(datediff(
                    fd.feature_date,
                    MAX(CASE WHEN CAST(m.completed_at AS DATE) < fd.feature_date
                             THEN CAST(m.completed_at AS DATE) END)
                ), 999) AS days_since_last_maintenance,

                COALESCE(datediff(
                    fd.feature_date,
                    MAX(CASE WHEN CAST(m.completed_at AS DATE) < fd.feature_date
                              AND m.failure_detected = TRUE
                             THEN CAST(m.completed_at AS DATE) END)
                ), 999) AS days_since_last_failure,

                COUNT(CASE WHEN CAST(m.completed_at AS DATE) >= date_sub(fd.feature_date, 30)
                            AND CAST(m.completed_at AS DATE) <  fd.feature_date THEN 1 END) AS maint_count_30d,

                COUNT(CASE WHEN CAST(m.completed_at AS DATE) >= date_sub(fd.feature_date, 90)
                            AND CAST(m.completed_at AS DATE) <  fd.feature_date THEN 1 END) AS maint_count_90d,

                COALESCE(SUM(CASE WHEN CAST(m.completed_at AS DATE) >= date_sub(fd.feature_date, 30)
                                   AND CAST(m.completed_at AS DATE) <  fd.feature_date
                                  THEN m.downtime_minutes END), 0) AS downtime_30d,

                COUNT(CASE WHEN m.failure_detected = TRUE
                            AND CAST(m.completed_at AS DATE) >= date_sub(fd.feature_date, 90)
                            AND CAST(m.completed_at AS DATE) <  fd.feature_date THEN 1 END) AS failure_count_90d,

                COUNT(CASE WHEN m.maintenance_type = 'corrective'
                            AND CAST(m.completed_at AS DATE) >= date_sub(fd.feature_date, 90)
                            AND CAST(m.completed_at AS DATE) <  fd.feature_date THEN 1 END) AS corrective_count_90d

            FROM feature_dates fd
            LEFT JOIN maintenance_events m ON m.robot_id = fd.serial_number
            GROUP BY fd.serial_number, fd.feature_date
        ),

        status_feat AS (
            SELECT
                fd.serial_number,
                fd.feature_date,
                MAX(STRUCT(sd.dt, sd.battery_cycle_times)).battery_cycle_times AS battery_cycle_times_last,
                MAX(STRUCT(sd.dt, sd.battery_health)).battery_health            AS battery_health_last,
                AVG(sd.avg_battery_soc)                                         AS avg_battery_soc_7d
            FROM feature_dates fd
            JOIN status_daily sd
                ON  sd.serial_number = fd.serial_number
                AND sd.dt <  fd.feature_date
                AND sd.dt >= date_sub(fd.feature_date, 7)
            GROUP BY fd.serial_number, fd.feature_date
        ),

        target AS (
            SELECT
                fd.serial_number,
                fd.feature_date,
                COALESCE(MAX(CASE WHEN m.failure_detected = TRUE THEN 1 ELSE 0 END), 0) AS has_failure_next_30d
            FROM feature_dates fd
            LEFT JOIN maintenance_events m
                ON  m.robot_id = fd.serial_number
                AND CAST(m.completed_at AS DATE) >  fd.feature_date
                AND CAST(m.completed_at AS DATE) <= date_add(fd.feature_date, 30)
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
            t.has_failure_next_30d
        FROM task_feat tf
        LEFT JOIN maint_feat  mf ON mf.serial_number = tf.serial_number AND mf.feature_date = tf.feature_date
        LEFT JOIN status_feat sf ON sf.serial_number = tf.serial_number AND sf.feature_date = tf.feature_date
        LEFT JOIN target       t ON  t.serial_number = tf.serial_number AND  t.feature_date = tf.feature_date
        ORDER BY tf.serial_number, tf.feature_date
    """)

    gold_df.coalesce(1).write.mode("overwrite").parquet(f"{gold}/robot_features")

    count = gold_df.count()
    print(f"Gold dataset: {count} rows × {len(gold_df.columns)} columns")
    return count


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: gold_job.py <silver_path> <gold_path>")
        sys.exit(1)

    silver, gold = sys.argv[1], sys.argv[2]
    spark = build_spark()

    print(f"Silver : {silver}")
    print(f"Gold   : {gold}")

    build_gold(spark, silver, gold)

    print("=== Gold complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
