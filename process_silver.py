"""
Silver layer: Bronze → нормализованные Parquet-партиции.

Для каждого из трёх наборов данных:
  • читает все Bronze-партиции (source=…/load_dt=…)
  • нормализует типы, переименовывает поля в snake_case, дедуплицирует
  • записывает Parquet с Hive-партиционированием по dt

Запуск:
    python3 process_silver.py
    python3 process_silver.py --bronze ./data/bronze --silver ./data/silver
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _write_parquet(
    con: duckdb.DuckDBPyConnection,
    select_sql: str,
    out_dir: Path,
) -> int:
    """
    Исполняет select_sql через временную таблицу, пишет Parquet с
    партиционированием по колонке `dt`. Возвращает количество записей.
    Очищает out_dir перед записью — запуск идемпотентен.
    """
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    con.execute(f"CREATE OR REPLACE TABLE _silver_tmp AS ({select_sql})")
    count: int = con.execute("SELECT COUNT(*) FROM _silver_tmp").fetchone()[0]
    con.execute(f"""
        COPY _silver_tmp
        TO '{out_dir}'
        (FORMAT PARQUET, PARTITION_BY (dt), OVERWRITE_OR_IGNORE TRUE)
    """)
    con.execute("DROP TABLE _silver_tmp")
    return count


def _partition_count(directory: Path) -> int:
    return len(list(directory.glob("dt=*")))


# ---------------------------------------------------------------------------
# Трансформации
# ---------------------------------------------------------------------------

def silver_maintenance_events(
    con: duckdb.DuckDBPyConnection,
    bronze: Path,
    silver: Path,
) -> int:
    """
    Bronze maintenance_events → Silver.

    Нормализация:
      • auto_detect раскрывает JSON в типизированный STRUCT[]
      • failure_code, comment: JSON (nullable) → VARCHAR
      • downtime_minutes: BIGINT → INTEGER
      • фильтрует: только status = 'completed'
      • дедуплицирует по maintenance_id (последняя версия по updated_at)
    Партиция: dt = DATE(completed_at)
    """
    src = str(bronze / "maintenance_events/source=synthetic/load_dt=*/*.json")
    out = silver / "maintenance_events"

    sql = f"""
    WITH deduped AS (
        SELECT
            rec.maintenance_id,
            rec.robot_id,
            rec.service_ticket_id,
            rec.service_source,
            rec.maintenance_type,
            rec.maintenance_category,
            rec.maintenance_status,

            rec.reported_at,
            rec.started_at,
            rec.completed_at,

            rec.severity,
            rec.failure_detected,
            rec.failure_code::VARCHAR    AS failure_code,
            rec.component,
            rec.action_taken,
            rec.parts_replaced,             -- уже VARCHAR[]

            rec.downtime_minutes::INTEGER AS downtime_minutes,
            rec.technician_id,
            rec.comment::VARCHAR          AS comment,

            rec.created_at,
            rec.updated_at,

            rec.completed_at::DATE        AS dt

        FROM (
            SELECT UNNEST(data) AS rec
            FROM read_json('{src}', auto_detect = true)
        )
        WHERE rec.maintenance_status = 'completed'
          AND rec.completed_at       IS NOT NULL

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY rec.maintenance_id
            ORDER BY rec.updated_at DESC
        ) = 1
    )
    SELECT * FROM deduped
    ORDER BY completed_at, robot_id
    """

    return _write_parquet(con, sql, out)


def silver_task_reports(
    con: duckdb.DuckDBPyConnection,
    bronze: Path,
    silver: Path,
) -> int:
    """
    Bronze task_reports → Silver.

    Нормализация:
      • двойной UNNEST: data (страницы) → robotTaskReports (отчёты)
      • id: UUID → VARCHAR
      • числовые поля BIGINT → INTEGER
      • добавляет вычисляемый battery_delta_pct
      • дедуплицирует по report_id
    Партиция: dt = DATE(start_time)
    """
    src = str(bronze / "task_reports/source=gausium/load_dt=*/*.json")
    out = silver / "task_reports"

    sql = f"""
    WITH deduped AS (
        SELECT
            report.id::VARCHAR                                       AS report_id,
            report.robotSerialNumber                                 AS robot_serial_number,
            report.robot                                             AS robot_display_name,
            report."operator"                                        AS operator,
            report.areaNameList                                      AS map_name,
            report.cleaningMode                                      AS cleaning_mode,
            report.taskEndStatus         ::INTEGER                   AS task_end_status,
            report.completionPercentage                                      AS completion_percentage,
            report.durationSeconds       ::INTEGER                   AS duration_seconds,

            report.plannedCleaningAreaSquareMeter                    AS planned_area_m2,
            report.actualCleaningAreaSquareMeter                     AS actual_area_m2,
            report.efficiencySquareMeterPerHour                      AS efficiency_m2_per_hour,
            report.waterConsumptionLiter                             AS water_consumption_l,

            report.startBatteryPercentage::INTEGER                   AS start_battery_pct,
            report.endBatteryPercentage  ::INTEGER                   AS end_battery_pct,
            -- вычисляемый признак: изменение заряда батареи за миссию
            report.endBatteryPercentage::INTEGER
              - report.startBatteryPercentage::INTEGER               AS battery_delta_pct,

            -- остаток ресурса расходников в %
            report.consumablesResidualPercentage.brush   ::INTEGER   AS brush_residual_pct,
            report.consumablesResidualPercentage."filter"::INTEGER   AS filter_residual_pct,
            report.consumablesResidualPercentage.suctionBlade::INTEGER AS suction_blade_residual_pct,

            report.startTime                                         AS start_time,
            report.endTime                                           AS end_time,

            report.startTime::DATE                                   AS dt

        FROM (
            SELECT UNNEST(page.robotTaskReports) AS report
            FROM (
                SELECT UNNEST(data) AS page
                FROM read_json('{src}', auto_detect = true)
            )
        )
        WHERE report.id        IS NOT NULL
          AND report.startTime IS NOT NULL

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY report.id
            ORDER BY report.startTime
        ) = 1
    )
    SELECT * FROM deduped
    ORDER BY start_time, robot_serial_number
    """

    return _write_parquet(con, sql, out)


def silver_robot_status(
    con: duckdb.DuckDBPyConnection,
    bronze: Path,
    silver: Path,
) -> int:
    """
    Bronze robot_status (JSONL) → Silver.

    Нормализация:
      • auto_detect раскрывает вложенные объекты в типизированные STRUCT
      • выравнивает battery, device, localizationInfo
      • переименовывает поля в snake_case
      • убирает технические Bronze-поля (_source, _batch_id, _schema, _load_dt)
      • числовые поля BIGINT → INTEGER
      • дедуплицирует по (serial_number, poll_ts)
    Партиция: dt = DATE(poll_ts)
    """
    src = str(bronze / "robot_status/source=gausium/load_dt=*/*.jsonl")
    out = silver / "robot_status"

    sql = f"""
    WITH deduped AS (
        SELECT
            serialNumber                                AS serial_number,
            _poll_ts                                    AS poll_ts,
            taskState                                   AS task_state,
            online,
            speedKilometerPerHour                       AS speed_kmh,

            -- батарея
            battery.soc          ::INTEGER              AS battery_soc,
            battery.charging                            AS battery_charging,
            battery.cycleTimes   ::INTEGER              AS battery_cycle_times,
            battery.soh                                 AS battery_health,
            battery.temperature1 ::INTEGER              AS battery_temp1,
            battery.temperature2 ::INTEGER              AS battery_temp2,
            battery.totalVoltage ::INTEGER              AS total_voltage_mv,

            -- навигация и позиция
            localizationInfo.localizationState          AS localization_state,
            localizationInfo.map.id                     AS map_id,
            localizationInfo.map.name                   AS map_name,
            localizationInfo.mapPosition.x ::INTEGER    AS pos_x,
            localizationInfo.mapPosition.y ::INTEGER    AS pos_y,
            navStatus                                   AS nav_status,

            -- уровни баков
            device.cleanWaterTank.level    ::INTEGER    AS clean_water_tank_pct,
            device.recoveryWaterTank.level ::INTEGER    AS recovery_water_tank_pct,

            -- износ расходников (часы эксплуатации)
            device.rollingBrush.usedLife                AS rolling_brush_used_life_h,
            device.rollingBrush.lifeSpan   ::INTEGER    AS rolling_brush_lifespan_h,
            device.rightSideBrush.usedLife              AS right_side_brush_used_life_h,
            device.softSqueegee.usedLife                AS soft_squeegee_used_life_h,
            device.cleanWaterFilter.usedLife            AS clean_water_filter_used_life_h,
            device.hepaSensor.usedLife                  AS hepa_sensor_used_life_h,

            _poll_ts::DATE                              AS dt

        FROM read_json(
            '{src}',
            format        = 'newline_delimited',
            auto_detect   = true,
            maximum_depth = 5
        )
        WHERE _poll_ts     IS NOT NULL
          AND serialNumber IS NOT NULL

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY serialNumber, _poll_ts
            ORDER BY _poll_ts
        ) = 1
    )
    SELECT * FROM deduped
    ORDER BY poll_ts, serial_number
    """

    return _write_parquet(con, sql, out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver: нормализация и типизация данных"
    )
    parser.add_argument(
        "--bronze", default="./data/bronze",
        help="Bronze root (default: ./data/bronze)",
    )
    parser.add_argument(
        "--silver", default="./data/silver",
        help="Silver root (default: ./data/silver)",
    )
    args = parser.parse_args()

    bronze = Path(args.bronze)
    silver = Path(args.silver)

    if not bronze.exists():
        raise SystemExit(f"Bronze-каталог не найден: {bronze.resolve()}")

    print(f"Bronze : {bronze.resolve()}")
    print(f"Silver : {silver.resolve()}")
    print()

    con = duckdb.connect()
    results: dict[str, int] = {}

    print("  [1/3] maintenance_events ...", end=" ", flush=True)
    results["maintenance_events"] = silver_maintenance_events(con, bronze, silver)
    parts = _partition_count(silver / "maintenance_events")
    print(f"{results['maintenance_events']:>6} records  ({parts} dt-партиций)")

    print("  [2/3] task_reports       ...", end=" ", flush=True)
    results["task_reports"] = silver_task_reports(con, bronze, silver)
    parts = _partition_count(silver / "task_reports")
    print(f"{results['task_reports']:>6} records  ({parts} dt-партиций)")

    print("  [3/3] robot_status       ...", end=" ", flush=True)
    results["robot_status"] = silver_robot_status(con, bronze, silver)
    parts = _partition_count(silver / "robot_status")
    print(f"{results['robot_status']:>6} records  ({parts} dt-партиций)")

    print()
    print("=== Silver layer complete ===")
    for name, count in results.items():
        parts = _partition_count(silver / name)
        size_kb = sum(
            f.stat().st_size for f in (silver / name).rglob("*.parquet")
        ) // 1024
        print(f"  {name:<25} {count:>8} records  {parts:>3} partitions  {size_kb:>6} KB")
    total = sum(results.values())
    print(f"  {'total':<25} {total:>8} records")
    print(f"\n  Output: {silver.resolve()}")


if __name__ == "__main__":
    main()
