"""
PySpark job: Raw/Bronze JSON/JSONL → нормализованные ODS/Silver Parquet-таблицы.

Файл нормализует операционные данные роботов:
    1. maintenance_events — события обслуживания и ремонтов;
    2. task_reports       — отчёты о выполненных заданиях;
    3. robot_status       — телеметрия и статусы роботов.

Запуск:
    spark-submit normalize_robot_operational_data.py \
        --maintenance-events-path s3a://robopulse/bronze/maintenance_events/source=synthetic/load_dt=2026-04-30/*.json \
        --task-reports-path s3a://robopulse/bronze/task_reports/source=gausium/load_dt=2026-04-30/*.json \
        --robot-status-path s3a://robopulse/bronze/robot_status/source=gausium/load_dt=2026-04-30/*.jsonl \
        --silver-path s3a://robopulse/silver \
        --partition-dt 2026-04-30
"""

import argparse
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
    "Usage: normalize_robot_operational_data.py "
    "--maintenance-events-path <path> "
    "--task-reports-path <path> "
    "--robot-status-path <path> "
    "--silver-path <path> "
    "--partition-dt <YYYY-MM-DD>"
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


def require_existing_paths(
    spark: SparkSession,
    paths_arg: str,
    dataset_name: str,
) -> list[str]:
    """
    Проверяет, что обязательная входная партиция существует.

    Args:
        spark: Активная SparkSession.
        paths_arg: Один путь или список путей через запятую.
        dataset_name: Имя датасета для понятного сообщения об ошибке.

    Returns:
        Список существующих путей.
    """
    existing_paths = resolve_existing_paths(spark, paths_arg)
    if not existing_paths:
        raise FileNotFoundError(
            f"Не найдена входная партиция {dataset_name}: {paths_arg}"
        )

    return existing_paths


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
    records_count = df.count()
    if records_count == 0:
        return 0

    df.sparkSession.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic",
    )
    df.write.mode("overwrite").partitionBy(partition_column).parquet(output_path)
    return records_count


def normalize_maintenance_events(
    spark: SparkSession,
    source_path: str,
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
    output_path = f"{silver_path}/{MAINTENANCE_EVENTS_TABLE}"
    source_paths = resolve_existing_paths(spark, source_path)
    if not source_paths:
        print(f"maintenance_events: пустая партиция {source_path}")
        return 0

    df = (
        spark.read.option("multiLine", True)
        .json(source_paths)
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
    source_path: str,
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
    output_path = f"{silver_path}/{TASK_REPORTS_TABLE}"
    source_paths = require_existing_paths(
        spark=spark,
        paths_arg=source_path,
        dataset_name=TASK_REPORTS_TABLE,
    )

    start_battery_pct = F.col("report.startBatteryPercentage").cast("integer")
    end_battery_pct = F.col("report.endBatteryPercentage").cast("integer")

    df = (
        spark.read.option("multiLine", True)
        .json(source_paths)
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
    source_path: str,
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
    output_path = f"{silver_path}/{ROBOT_STATUS_TABLE}"
    source_paths = require_existing_paths(
        spark=spark,
        paths_arg=source_path,
        dataset_name=ROBOT_STATUS_TABLE,
    )

    df = (
        spark.read.option("mode", "DROPMALFORMED")
        .json(source_paths)
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
    maintenance_events_path: str,
    task_reports_path: str,
    robot_status_path: str,
    silver_path: str,
    partition_dt: str,
) -> None:
    """
    Запускает полный пайплайн нормализации операционных данных роботов.

    Args:
        spark: Активная SparkSession.
        maintenance_events_path: Путь к Bronze-партиции maintenance_events.
        task_reports_path: Путь к Bronze-партиции task_reports.
        robot_status_path: Путь к Bronze-партиции robot_status.
        silver_path: Путь к ODS/Silver-слою.
        partition_dt: Дата обрабатываемой бизнес-партиции.
    """
    print(f"Partition          : {partition_dt}")
    print(f"Maintenance events : {maintenance_events_path}")
    print(f"Task reports       : {task_reports_path}")
    print(f"Robot status       : {robot_status_path}")
    print(f"Silver             : {silver_path}")

    jobs = [
        (
            "maintenance_events",
            normalize_maintenance_events,
            maintenance_events_path,
        ),
        ("task_reports", normalize_task_reports, task_reports_path),
        ("robot_status", normalize_robot_status, robot_status_path),
    ]

    for index, (job_name, job_func, source_path) in enumerate(jobs, start=1):
        print(f"[{index}/{len(jobs)}] {job_name} ...", flush=True)

        records_count = job_func(
            spark=spark,
            source_path=source_path,
            silver_path=silver_path,
        )

        print(f"{records_count} records")


def parse_args() -> argparse.Namespace:
    """
    Разбирает аргументы CLI и сохраняет старый двухаргументный режим.

    Returns:
        Namespace с путями входных партиций и Silver-слоя.
    """
    if len(sys.argv) == 3 and not sys.argv[1].startswith("--"):
        bronze_path, silver_path = sys.argv[1], sys.argv[2]
        return argparse.Namespace(
            maintenance_events_path=(
                f"{bronze_path}/maintenance_events/"
                "source=synthetic/load_dt=*/*.json"
            ),
            task_reports_path=(
                f"{bronze_path}/task_reports/"
                "source=gausium/load_dt=*/*.json"
            ),
            robot_status_path=(
                f"{bronze_path}/robot_status/"
                "source=gausium/load_dt=*/*.jsonl"
            ),
            silver_path=silver_path,
            partition_dt="*",
        )

    parser = argparse.ArgumentParser(description=USAGE_MESSAGE)
    parser.add_argument("--maintenance-events-path", required=True)
    parser.add_argument("--task-reports-path", required=True)
    parser.add_argument("--robot-status-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--partition-dt", required=True)
    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint для spark-submit.
    """
    args = parse_args()

    spark = build_spark()

    try:
        run_pipeline(
            spark=spark,
            maintenance_events_path=args.maintenance_events_path,
            task_reports_path=args.task_reports_path,
            robot_status_path=args.robot_status_path,
            silver_path=args.silver_path,
            partition_dt=args.partition_dt,
        )
        print("=== Silver complete ===")
    finally:
        # SparkSession нужно закрывать даже при ошибках,
        # чтобы корректно освобождались ресурсы executors/driver.
        spark.stop()


if __name__ == "__main__":
    main()
