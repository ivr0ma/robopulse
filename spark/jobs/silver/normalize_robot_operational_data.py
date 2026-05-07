"""
PySpark job: Raw/Bronze JSON/JSONL → нормализованные ODS/Silver Parquet-таблицы.

Файл нормализует операционные данные роботов:
    1. maintenance_events — события обслуживания и ремонтов;
    2. task_reports       — отчёты о выполненных заданиях;
    3. robot_status       — телеметрия и статусы роботов.

Запуск:
    spark-submit normalize_robot_operational_data.py \
        s3a://robopulse/bronze \
        s3a://robopulse/silver
"""

import sys
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.window import Window

from spark.common.spark_session import build_spark


# Названия выходных ODS/Silver-таблиц.
MAINTENANCE_EVENTS_TABLE = "maintenance_events"
TASK_REPORTS_TABLE = "task_reports"
ROBOT_STATUS_TABLE = "robot_status"

# Техническая колонка партиционирования.
PARTITION_COLUMN = "dt"

USAGE_MESSAGE = (
    "Usage: normalize_robot_operational_data.py <bronze_path> <silver_path>"
)


def deduplicate_by_row_number(
    df: DataFrame,
    partition_by: Sequence[str],
    order_by: Sequence[Column | str],
) -> DataFrame:
    """
    Удаляет дубли, оставляя одну запись внутри каждой группы.

    Логика:
        1. Группируем записи по бизнес-ключу.
        2. Сортируем внутри группы.
        3. Оставляем запись с row_number = 1.

    Args:
        df: Исходный DataFrame.
        partition_by: Колонки, задающие группу дублей.
        order_by: Правила сортировки внутри группы.

    Returns:
        DataFrame без дублей.
    """
    window = Window.partitionBy(*partition_by).orderBy(*order_by)

    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_partitioned_parquet(
    df: DataFrame,
    output_path: str,
    partition_column: str = PARTITION_COLUMN,
) -> int:
    """
    Записывает DataFrame в Parquet с партиционированием по дате.

    Args:
        df: DataFrame для записи.
        output_path: Путь назначения.
        partition_column: Колонка партиционирования.

    Returns:
        Количество записанных строк.
    """
    df.write.mode("overwrite").partitionBy(partition_column).parquet(output_path)
    return df.count()


def normalize_maintenance_events(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> int:
    """
    Нормализует события обслуживания роботов.

    На входе:
        Raw/Bronze JSON с массивом data.

    На выходе:
        Silver/ODS-таблица maintenance_events.

    Бизнес-логика:
        - оставляем только завершённые обслуживания;
        - исключаем записи без completed_at;
        - приводим типы;
        - дедуплицируем по maintenance_id;
        - для дублей оставляем самую свежую версию по updated_at.
    """
    source_path = (
        f"{bronze_path}/maintenance_events/source=synthetic/load_dt=*/*.json"
    )
    output_path = f"{silver_path}/{MAINTENANCE_EVENTS_TABLE}"

    df = (
        spark.read.option("multiLine", True)
        .json(source_path)
        # В raw-файле бизнес-записи лежат внутри массива data.
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
            F.col("rec.downtime_minutes").cast("integer").alias(
                "downtime_minutes"
            ),
            F.col("rec.technician_id"),
            F.col("rec.comment").cast("string").alias("comment"),
            F.col("rec.created_at"),
            F.col("rec.updated_at"),
            F.to_date(F.col("rec.completed_at")).alias(PARTITION_COLUMN),
        )
        # В ODS попадают только фактически завершённые обслуживания.
        .filter(
            (F.col("maintenance_status") == "completed")
            & F.col("completed_at").isNotNull()
        )
    )

    df = deduplicate_by_row_number(
        df=df,
        partition_by=["maintenance_id"],
        order_by=[F.col("updated_at").desc()],
    ).orderBy("completed_at", "robot_id")

    return write_partitioned_parquet(df, output_path)


def normalize_task_reports(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> int:
    """
    Нормализует отчёты о выполненных заданиях роботов.

    На входе:
        Raw/Bronze JSON с постраничной структурой:
            data[].robotTaskReports[]

    На выходе:
        Silver/ODS-таблица task_reports.

    Бизнес-логика:
        - разворачиваем страницы и отчёты;
        - приводим типы;
        - считаем изменение заряда батареи за задание;
        - исключаем записи без report_id и start_time;
        - дедуплицируем по report_id.
    """
    source_path = (
        f"{bronze_path}/task_reports/source=gausium/load_dt=*/*.json"
    )
    output_path = f"{silver_path}/{TASK_REPORTS_TABLE}"

    start_battery_pct = F.col("report.startBatteryPercentage").cast("integer")
    end_battery_pct = F.col("report.endBatteryPercentage").cast("integer")

    df = (
        spark.read.option("multiLine", True)
        .json(source_path)
        # data содержит страницы ответа API.
        .select(F.explode("data").alias("page"))
        # Каждая страница содержит список отчётов по заданиям роботов.
        .select(F.explode("page.robotTaskReports").alias("report"))
        .select(
            F.col("report.id").cast("string").alias("report_id"),
            F.col("report.robotSerialNumber").alias("robot_serial_number"),
            F.col("report.robot").alias("robot_display_name"),
            F.col("report.operator").alias("operator"),
            F.col("report.areaNameList").alias("map_name"),
            F.col("report.cleaningMode").alias("cleaning_mode"),
            F.col("report.taskEndStatus").cast("integer").alias(
                "task_end_status"
            ),
            F.col("report.completionPercentage").alias(
                "completion_percentage"
            ),
            F.col("report.durationSeconds").cast("integer").alias(
                "duration_seconds"
            ),
            F.col("report.plannedCleaningAreaSquareMeter").alias(
                "planned_area_m2"
            ),
            F.col("report.actualCleaningAreaSquareMeter").alias(
                "actual_area_m2"
            ),
            F.col("report.efficiencySquareMeterPerHour").alias(
                "efficiency_m2_per_hour"
            ),
            F.col("report.waterConsumptionLiter").alias(
                "water_consumption_l"
            ),
            start_battery_pct.alias("start_battery_pct"),
            end_battery_pct.alias("end_battery_pct"),
            (end_battery_pct - start_battery_pct).alias("battery_delta_pct"),
            F.col("report.consumablesResidualPercentage.brush")
            .cast("integer")
            .alias("brush_residual_pct"),
            F.col("report.consumablesResidualPercentage.`filter`")
            .cast("integer")
            .alias("filter_residual_pct"),
            F.col("report.consumablesResidualPercentage.suctionBlade")
            .cast("integer")
            .alias("suction_blade_residual_pct"),
            F.col("report.startTime").alias("start_time"),
            F.col("report.endTime").alias("end_time"),
            F.to_date(F.col("report.startTime")).alias(PARTITION_COLUMN),
        )
        # Без идентификатора отчёта и даты старта запись нельзя корректно
        # использовать в ODS и последующих витринах.
        .filter(
            F.col("report_id").isNotNull()
            & F.col("start_time").isNotNull()
        )
    )

    df = deduplicate_by_row_number(
        df=df,
        partition_by=["report_id"],
        order_by=[F.col("start_time")],
    ).orderBy("start_time", "robot_serial_number")

    return write_partitioned_parquet(df, output_path)


def normalize_robot_status(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> int:
    """
    Нормализует телеметрию и статусы роботов.

    На входе:
        Raw/Bronze JSONL, где каждая строка — отдельный снимок статуса.

    На выходе:
        Silver/ODS-таблица robot_status.

    Бизнес-логика:
        - читаем JSONL в режиме DROPMALFORMED;
        - извлекаем техническое состояние робота, батарею, навигацию,
          карту и состояние расходников;
        - исключаем записи без времени опроса и серийного номера;
        - дедуплицируем по паре serial_number + poll_ts.
    """
    source_path = (
        f"{bronze_path}/robot_status/source=gausium/load_dt=*/*.jsonl"
    )
    output_path = f"{silver_path}/{ROBOT_STATUS_TABLE}"

    df = (
        spark.read.option("mode", "DROPMALFORMED")
        .json(source_path)
        .select(
            F.col("serialNumber").alias("serial_number"),
            F.col("_poll_ts").alias("poll_ts"),
            F.col("taskState").alias("task_state"),
            F.col("online"),
            F.col("speedKilometerPerHour").alias("speed_kmh"),
            F.col("battery.soc").cast("integer").alias("battery_soc"),
            F.col("battery.charging").alias("battery_charging"),
            F.col("battery.cycleTimes").cast("integer").alias(
                "battery_cycle_times"
            ),
            F.col("battery.soh").alias("battery_health"),
            F.col("battery.temperature1").cast("integer").alias(
                "battery_temp1"
            ),
            F.col("battery.temperature2").cast("integer").alias(
                "battery_temp2"
            ),
            F.col("battery.totalVoltage").cast("integer").alias(
                "total_voltage_mv"
            ),
            F.col("localizationInfo.localizationState").alias(
                "localization_state"
            ),
            F.col("localizationInfo.map.id").alias("map_id"),
            F.col("localizationInfo.map.name").alias("map_name"),
            F.col("localizationInfo.mapPosition.x")
            .cast("integer")
            .alias("pos_x"),
            F.col("localizationInfo.mapPosition.y")
            .cast("integer")
            .alias("pos_y"),
            F.col("navStatus").alias("nav_status"),
            F.col("device.cleanWaterTank.level")
            .cast("integer")
            .alias("clean_water_tank_pct"),
            F.col("device.recoveryWaterTank.level")
            .cast("integer")
            .alias("recovery_water_tank_pct"),
            F.col("device.rollingBrush.usedLife").alias(
                "rolling_brush_used_life_h"
            ),
            F.col("device.rollingBrush.lifeSpan")
            .cast("integer")
            .alias("rolling_brush_lifespan_h"),
            F.col("device.rightSideBrush.usedLife").alias(
                "right_side_brush_used_life_h"
            ),
            F.col("device.softSqueegee.usedLife").alias(
                "soft_squeegee_used_life_h"
            ),
            F.col("device.cleanWaterFilter.usedLife").alias(
                "clean_water_filter_used_life_h"
            ),
            F.col("device.hepaSensor.usedLife").alias(
                "hepa_sensor_used_life_h"
            ),
            F.to_date(F.col("_poll_ts")).alias(PARTITION_COLUMN),
        )
        # Серийный номер и timestamp опроса — минимальный ключ записи статуса.
        .filter(
            F.col("poll_ts").isNotNull()
            & F.col("serial_number").isNotNull()
        )
    )

    df = deduplicate_by_row_number(
        df=df,
        partition_by=["serial_number", "poll_ts"],
        order_by=[F.col("poll_ts")],
    ).orderBy("poll_ts", "serial_number")

    return write_partitioned_parquet(df, output_path)


def run_pipeline(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
) -> None:
    """
    Запускает полный пайплайн нормализации операционных данных роботов.

    Args:
        spark: Активная SparkSession.
        bronze_path: Путь к Raw/Bronze-слою.
        silver_path: Путь к ODS/Silver-слою.
    """
    print(f"Bronze : {bronze_path}")
    print(f"Silver : {silver_path}")

    jobs = [
        ("maintenance_events", normalize_maintenance_events),
        ("task_reports", normalize_task_reports),
        ("robot_status", normalize_robot_status),
    ]

    for index, (job_name, job_func) in enumerate(jobs, start=1):
        print(f"[{index}/{len(jobs)}] {job_name} ...", flush=True)

        records_count = job_func(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
        )

        print(f"{records_count} records")


def main() -> None:
    """
    CLI entrypoint для spark-submit.

    Ожидает два аргумента:
        1. путь к Bronze/Raw-слою;
        2. путь к Silver/ODS-слою.
    """
    if len(sys.argv) != 3:
        print(USAGE_MESSAGE)
        sys.exit(1)

    bronze_path, silver_path = sys.argv[1], sys.argv[2]
    spark = build_spark()

    try:
        run_pipeline(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
        )
        print("=== Silver complete ===")
    finally:
        # SparkSession нужно закрывать даже при ошибках,
        # чтобы корректно освобождались ресурсы executors/driver.
        spark.stop()


if __name__ == "__main__":
    main()
