"""
PySpark Silver job: Bronze (JSON/JSONL в S3) → нормализованные Parquet-партиции.

Запуск:
    spark-submit silver_job.py s3a://robopulse/bronze s3a://robopulse/silver
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("robopulse-silver").getOrCreate()


def silver_maintenance_events(spark: SparkSession, bronze: str, silver: str) -> int:
    src = f"{bronze}/maintenance_events/source=synthetic/load_dt=*/*.json"

    df = (
        spark.read.option("multiLine", True).json(src)
        .select(F.explode("data").alias("rec"))
        .select(
            F.col("rec.maintenance_id"),
            F.col("rec.robot_id"),
            F.col("rec.service_ticket_id"),
            F.col("rec.service_source"),
            F.col("rec.maintenance_type"),
            F.col("rec.maintenance_category"),
            F.col("rec.maintenance_status"),
            F.col("rec.reported_at"),
            F.col("rec.started_at"),
            F.col("rec.completed_at"),
            F.col("rec.severity"),
            F.col("rec.failure_detected"),
            F.col("rec.failure_code").cast("string").alias("failure_code"),
            F.col("rec.component"),
            F.col("rec.action_taken"),
            F.col("rec.parts_replaced"),
            F.col("rec.downtime_minutes").cast("integer").alias("downtime_minutes"),
            F.col("rec.technician_id"),
            F.col("rec.comment").cast("string").alias("comment"),
            F.col("rec.created_at"),
            F.col("rec.updated_at"),
            F.to_date(F.col("rec.completed_at")).alias("dt"),
        )
        .filter(
            (F.col("maintenance_status") == "completed") &
            F.col("completed_at").isNotNull()
        )
    )

    w = Window.partitionBy("maintenance_id").orderBy(F.col("updated_at").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .orderBy("completed_at", "robot_id")
    )

    df.write.mode("overwrite").partitionBy("dt").parquet(f"{silver}/maintenance_events")
    return df.count()


def silver_task_reports(spark: SparkSession, bronze: str, silver: str) -> int:
    src = f"{bronze}/task_reports/source=gausium/load_dt=*/*.json"

    df = (
        spark.read.option("multiLine", True).json(src)
        .select(F.explode("data").alias("page"))
        .select(F.explode("page.robotTaskReports").alias("report"))
        .select(
            F.col("report.id").cast("string").alias("report_id"),
            F.col("report.robotSerialNumber").alias("robot_serial_number"),
            F.col("report.robot").alias("robot_display_name"),
            F.col("report.operator").alias("operator"),
            F.col("report.areaNameList").alias("map_name"),
            F.col("report.cleaningMode").alias("cleaning_mode"),
            F.col("report.taskEndStatus").cast("integer").alias("task_end_status"),
            F.col("report.completionPercentage").alias("completion_percentage"),
            F.col("report.durationSeconds").cast("integer").alias("duration_seconds"),
            F.col("report.plannedCleaningAreaSquareMeter").alias("planned_area_m2"),
            F.col("report.actualCleaningAreaSquareMeter").alias("actual_area_m2"),
            F.col("report.efficiencySquareMeterPerHour").alias("efficiency_m2_per_hour"),
            F.col("report.waterConsumptionLiter").alias("water_consumption_l"),
            F.col("report.startBatteryPercentage").cast("integer").alias("start_battery_pct"),
            F.col("report.endBatteryPercentage").cast("integer").alias("end_battery_pct"),
            (
                F.col("report.endBatteryPercentage").cast("integer") -
                F.col("report.startBatteryPercentage").cast("integer")
            ).alias("battery_delta_pct"),
            F.col("report.consumablesResidualPercentage.brush").cast("integer").alias("brush_residual_pct"),
            F.col("report.consumablesResidualPercentage.`filter`").cast("integer").alias("filter_residual_pct"),
            F.col("report.consumablesResidualPercentage.suctionBlade").cast("integer").alias("suction_blade_residual_pct"),
            F.col("report.startTime").alias("start_time"),
            F.col("report.endTime").alias("end_time"),
            F.to_date(F.col("report.startTime")).alias("dt"),
        )
        .filter(
            F.col("report_id").isNotNull() &
            F.col("start_time").isNotNull()
        )
    )

    w = Window.partitionBy("report_id").orderBy(F.col("start_time"))
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .orderBy("start_time", "robot_serial_number")
    )

    df.write.mode("overwrite").partitionBy("dt").parquet(f"{silver}/task_reports")
    return df.count()


def silver_robot_status(spark: SparkSession, bronze: str, silver: str) -> int:
    src = f"{bronze}/robot_status/source=gausium/load_dt=*/*.jsonl"

    df = (
        spark.read.option("mode", "DROPMALFORMED").json(src)
        .select(
            F.col("serialNumber").alias("serial_number"),
            F.col("_poll_ts").alias("poll_ts"),
            F.col("taskState").alias("task_state"),
            F.col("online"),
            F.col("speedKilometerPerHour").alias("speed_kmh"),
            F.col("battery.soc").cast("integer").alias("battery_soc"),
            F.col("battery.charging").alias("battery_charging"),
            F.col("battery.cycleTimes").cast("integer").alias("battery_cycle_times"),
            F.col("battery.soh").alias("battery_health"),
            F.col("battery.temperature1").cast("integer").alias("battery_temp1"),
            F.col("battery.temperature2").cast("integer").alias("battery_temp2"),
            F.col("battery.totalVoltage").cast("integer").alias("total_voltage_mv"),
            F.col("localizationInfo.localizationState").alias("localization_state"),
            F.col("localizationInfo.map.id").alias("map_id"),
            F.col("localizationInfo.map.name").alias("map_name"),
            F.col("localizationInfo.mapPosition.x").cast("integer").alias("pos_x"),
            F.col("localizationInfo.mapPosition.y").cast("integer").alias("pos_y"),
            F.col("navStatus").alias("nav_status"),
            F.col("device.cleanWaterTank.level").cast("integer").alias("clean_water_tank_pct"),
            F.col("device.recoveryWaterTank.level").cast("integer").alias("recovery_water_tank_pct"),
            F.col("device.rollingBrush.usedLife").alias("rolling_brush_used_life_h"),
            F.col("device.rollingBrush.lifeSpan").cast("integer").alias("rolling_brush_lifespan_h"),
            F.col("device.rightSideBrush.usedLife").alias("right_side_brush_used_life_h"),
            F.col("device.softSqueegee.usedLife").alias("soft_squeegee_used_life_h"),
            F.col("device.cleanWaterFilter.usedLife").alias("clean_water_filter_used_life_h"),
            F.col("device.hepaSensor.usedLife").alias("hepa_sensor_used_life_h"),
            F.to_date(F.col("_poll_ts")).alias("dt"),
        )
        .filter(
            F.col("poll_ts").isNotNull() &
            F.col("serial_number").isNotNull()
        )
    )

    w = Window.partitionBy("serial_number", "poll_ts").orderBy("poll_ts")
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .orderBy("poll_ts", "serial_number")
    )

    df.write.mode("overwrite").partitionBy("dt").parquet(f"{silver}/robot_status")
    return df.count()


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: silver_job.py <bronze_path> <silver_path>")
        sys.exit(1)

    bronze, silver = sys.argv[1], sys.argv[2]
    spark = build_spark()

    print(f"Bronze : {bronze}")
    print(f"Silver : {silver}")

    print("[1/3] maintenance_events ...", flush=True)
    n = silver_maintenance_events(spark, bronze, silver)
    print(f"       {n} records")

    print("[2/3] task_reports ...", flush=True)
    n = silver_task_reports(spark, bronze, silver)
    print(f"       {n} records")

    print("[3/3] robot_status ...", flush=True)
    n = silver_robot_status(spark, bronze, silver)
    print(f"       {n} records")

    print("=== Silver complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
