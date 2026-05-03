"""
Генератор синтетических данных для двух источников:
  1) Gausium (list_robots, robot_status snapshots, task_reports)
  2) maintenance_events (события ТО, зависящие от эксплуатации)

Схемы соответствуют примерам из list_robots.json / robot_status.json /
task_reports.json и описанию в maintenance_events.md.

Логика связи источников (см. п. 9 maintenance_events.md):
  накопленная эксплуатация робота
    -> вероятность события ТО
    -> тип события (preventive | corrective | ...)
    -> компонент (brush, filter, battery, ...)
    -> длительность простоя
После события ТО соответствующий счётчик расходника сбрасывается,
что отражается в robot_status и в consumablesResidualPercentage
последующих task_reports.

Запуск:
    python3 generate_data.py --days 180 --out ./data --seed 42
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Справочники
# ---------------------------------------------------------------------------

ROBOT_FLEET: list[dict[str, str]] = [
    # CB500 / CleanBot W1 — компактный робот-мойщик
    {"serialNumber": "CB500-2100-W1R-0001", "displayName": "БЦ Орбита (1эт-вход) CB500-0001",      "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v2.5.2-20251129", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0002", "displayName": "БЦ Орбита (1эт-кафетерий) CB500-0002", "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v2.5.2-20251129", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0003", "displayName": "БЦ Орбита (1эт-спортзал) CB500-0003",  "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v2.5.2-20251129", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0004", "displayName": "БЦ Орбита (3эт) CB500-0004",           "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v2.5.2-20251129", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0005", "displayName": "Гипермаркет Полярис CB500-0005",        "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v3.1.0-20260318", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0006", "displayName": "ТЦ Галактика Юг CB500-0006",           "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v3.1.2-20260203", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0007", "displayName": "ТЦ Галактика Центр CB500-0007",         "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v3.1.2-20260203", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0008", "displayName": "БЦ Энергия CB500-0008",                "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v2.1.3-20251020", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0009", "displayName": "ТЦ Меридиан CB500-0009",               "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v2.1.3-20251020", "hardwareVersion": "H1.3"},
    {"serialNumber": "CB500-2100-W1R-0010", "displayName": "Магазин Орион ЕКБ CB500-0010",         "modelFamilyCode": "W1",  "modelTypeCode": "CleanBot W1",   "softwareVersion": "v3.1.2-20260203", "hardwareVersion": "H1.3"},
    # CB300 / CleanBot W50H — крупный робот-мойщик для больших площадей
    {"serialNumber": "CB300-2200-W5R-0001", "displayName": "Отель Астра CB300-0001",                "modelFamilyCode": "W50", "modelTypeCode": "CleanBot W50H", "softwareVersion": "v3.1.0-20260123", "hardwareVersion": "H2.0"},
    {"serialNumber": "CB300-2200-W5R-0002", "displayName": "Банк Кассиопея CB300-0002",             "modelFamilyCode": "W50", "modelTypeCode": "CleanBot W50H", "softwareVersion": "v3.1.2-20260203", "hardwareVersion": "H2.0"},
]

CLEAN_MODES_W1   = ["WASH", "SWEEP", "VAC", "DUST"]  # режимы CleanBot W1
CLEAN_MODES_W50H = ["WASH", "SWEEP"]                  # режимы CleanBot W50H

OPERATORS = ["dispatcher_01", "auto_scheduler", "dispatcher_02", "service_tech"]

MAP_NAMES = {
    "CB500-2100-W1R-0001": [("map-orbit-1f-entry-001", "orbit-bc-1floor-entry")],
    "CB500-2100-W1R-0002": [("map-orbit-1f-cafe-001",  "orbit-bc-1floor-cafe")],
    "CB500-2100-W1R-0003": [("map-orbit-1f-gym-001",   "orbit-bc-1floor-gym")],
    "CB500-2100-W1R-0004": [("map-orbit-3f-main-001",  "orbit-bc-3floor-main")],
    "CB500-2100-W1R-0005": [("map-polaris-main-001",   "polaris-hypermarket-main")],
    "CB500-2100-W1R-0006": [("map-galaktika-s-001",    "galaktika-tc-south")],
    "CB500-2100-W1R-0007": [("map-galaktika-c-001",    "galaktika-tc-center")],
    "CB500-2100-W1R-0008": [("map-energia-main-001",   "energia-bc-main")],
    "CB500-2100-W1R-0009": [("map-meridian-main-001",  "meridian-tc-main")],
    "CB500-2100-W1R-0010": [("map-orion-ekb-001",      "orion-store-ekb")],
    "CB300-2200-W5R-0001": [("map-astra-main-001",     "astra-hotel-main")],
    "CB300-2200-W5R-0002": [("map-kassiopeya-001",     "kassiopeya-bank-main")],
}

# Сценарии «нагрузки» сайта — чтобы фоном различались разные локации
SITE_PROFILES = {
    "CB500-2100-W1R-0001": {"missions_per_day": (4, 8),  "planned_area": (240, 280)},
    "CB500-2100-W1R-0002": {"missions_per_day": (3, 6),  "planned_area": (300, 380)},
    "CB500-2100-W1R-0003": {"missions_per_day": (1, 3),  "planned_area": (180, 220)},
    "CB500-2100-W1R-0004": {"missions_per_day": (3, 6),  "planned_area": (350, 420)},
    "CB500-2100-W1R-0005": {"missions_per_day": (5, 10), "planned_area": (700, 900)},
    "CB500-2100-W1R-0006": {"missions_per_day": (4, 7),  "planned_area": (500, 600)},
    "CB500-2100-W1R-0007": {"missions_per_day": (5, 9),  "planned_area": (650, 800)},
    "CB500-2100-W1R-0008": {"missions_per_day": (2, 4),  "planned_area": (280, 350)},
    "CB500-2100-W1R-0009": {"missions_per_day": (4, 7),  "planned_area": (450, 550)},
    "CB500-2100-W1R-0010": {"missions_per_day": (3, 6),  "planned_area": (380, 460)},
    "CB300-2200-W5R-0001": {"missions_per_day": (5, 9),  "planned_area": (800, 1000)},
    "CB300-2200-W5R-0002": {"missions_per_day": (4, 8),  "planned_area": (700, 900)},
}

# Срок службы расходника (в часах эксплуатации) — соответствует lifeSpan в robot_status
CONSUMABLE_LIFESPANS = {
    "rollingBrush":     300,
    "rightSideBrush":   300,
    "softSqueegee":     300,
    "cleanWaterFilter": 600,
    "hepaSensor":       600,
}

# Распределение вероятностей отказов по компонентам (взвешенно)
FAILURE_CATALOG = [
    # (failure_code, component, action, parts_replaced, base_weight)
    ("BRUSH_WEAR",          "brush",         "brush_replacement",          ["brush"],         0.22),
    ("FILTER_CLOGGED",      "filter",        "filter_replacement",         ["filter"],        0.18),
    ("SUCTION_BLADE_WEAR",  "suction_blade", "suction_blade_replacement",  ["suction_blade"], 0.16),
    ("BATTERY_DEGRADATION", "battery",       "battery_check",              [],                0.10),
    ("WHEEL_BLOCKED",       "wheel",         "wheel_unblock",              [],                0.08),
    ("MOTOR_OVERLOAD",      "motor",         "motor_inspection",           [],                0.06),
    ("LIDAR_ERROR",         "lidar",         "lidar_recalibration",        [],                0.07),
    ("DOCKING_FAILURE",     "dock_station",  "dock_alignment",             [],                0.05),
    ("WATER_PUMP_FAILURE",  "pump",          "pump_replacement",           ["pump"],          0.04),
    ("SOFTWARE_ERROR",      "software",      "software_update",            [],                0.04),
]

PREVENTIVE_PLAN_DAYS = 60   # каждые ~60 дней — плановое ТО
INSPECTION_PROB_PER_DAY = 0.005  # редкие диагностики без ремонта

# Параметры опроса robot_status
POLL_INTERVAL_MINUTES = 15  # интервал polling: каждые 15 минут
POLL_START_HOUR = 6         # первый опрос в 06:00 UTC
POLL_END_HOUR   = 23        # последний опрос в 23:00 UTC
CHARGE_RATE_PER_POLL = 4    # % батареи восстанавливается за один интервал при зарядке
WATER_CONSUME_PER_POLL = 2  # % расхода воды за один интервал при работе


# ---------------------------------------------------------------------------
# Состояние робота, мутируемое по ходу симуляции
# ---------------------------------------------------------------------------

@dataclass
class RobotState:
    serial: str
    display_name: str
    model_family: str
    model_type: str
    software_version: str
    hardware_version: str
    # «накопленные» износы расходников (часы)
    used_life: dict[str, float] = field(default_factory=lambda: {k: 0.0 for k in CONSUMABLE_LIFESPANS})
    # последний снимок батареи
    soc: int = 100
    cycle_times: int = 0
    battery_health: str = "HEALTHY"
    # эксплуатационные счётчики (нужны для генерации ТО)
    total_missions: int = 0
    total_runtime_seconds: int = 0
    total_area_m2: float = 0.0
    failures_30d: int = 0
    last_maintenance_date: datetime | None = None
    last_failure_date: datetime | None = None
    # для последнего snapshot/task запоминаем, чтобы знать остаток батареи
    last_battery_pct: int = 100


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def iso(dt: datetime) -> str:
    """ISO 8601 в UTC c суффиксом Z, как в task_reports.json."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---- Bronze layout ---------------------------------------------------------

BRONZE_SCHEMA_VERSION = "bronze.v1"


def bronze_partition_dir(
    out_dir: Path, dataset: str, source: str, load_dt: str,
) -> Path:
    """
    Hive-style путь для Bronze:
        <out>/bronze/<dataset>/source=<source>/load_dt=<YYYY-MM-DD>/
    """
    return (
        out_dir / "bronze" / dataset
        / f"source={source}" / f"load_dt={load_dt}"
    )


def write_bronze(
    out_dir: Path, dataset: str, source: str, load_dt: str,
    filename: str, raw_payload: Any, *, batch_id: str,
) -> None:
    """
    Записывает один файл в Bronze-партицию.

    Конверт:
        {
          "_meta": { source, load_dt, ingest_ts, batch_id,
                     record_count, schema },
          "data":   <исходный payload «как пришёл»>
        }

    Никакой нормализации тут не делаем — это сырые данные.
    """
    record_count = (
        len(raw_payload) if isinstance(raw_payload, list)
        else 1
    )
    # ingest_ts симулируем: конец «дня загрузки» в UTC
    ingest_ts = iso(
        datetime.fromisoformat(load_dt).replace(
            hour=23, minute=30, tzinfo=timezone.utc,
        )
    )
    envelope = {
        "_meta": {
            "source":       source,
            "load_dt":      load_dt,
            "ingest_ts":    ingest_ts,
            "batch_id":     batch_id,
            "record_count": record_count,
            "schema":       BRONZE_SCHEMA_VERSION,
        },
        "data": raw_payload,
    }
    write_json(
        bronze_partition_dir(out_dir, dataset, source, load_dt) / filename,
        envelope,
    )


def write_bronze_jsonl(
    out_dir: Path, dataset: str, source: str, load_dt: str,
    filename: str, records: list[dict[str, Any]],
) -> None:
    """
    Записывает JSONL в Bronze-партицию.
    Каждая строка — самодостаточная запись с уже встроенными мета-полями _poll_ts,
    _load_dt, _source, _batch_id, _schema.
    """
    path = bronze_partition_dir(out_dir, dataset, source, load_dt) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def weighted_choice(pairs: list[tuple[Any, float]]) -> Any:
    total = sum(w for _, w in pairs)
    r = random.random() * total
    acc = 0.0
    for value, w in pairs:
        acc += w
        if r <= acc:
            return value
    return pairs[-1][0]


# ---------------------------------------------------------------------------
# Генерация задач (task_reports)
# ---------------------------------------------------------------------------

def generate_task(
    state: RobotState,
    start_dt: datetime,
    operator: str,
) -> tuple[dict[str, Any], int]:
    """Возвращает (task_report_dict, end_battery_pct)."""

    profile = SITE_PROFILES[state.serial]
    planned_lo, planned_hi = profile["planned_area"]

    planned = round(random.uniform(planned_lo, planned_hi), 3)

    # Чем сильнее износ — тем чаще миссии прерываются
    avg_wear = sum(
        state.used_life[k] / CONSUMABLE_LIFESPANS[k]
        for k in CONSUMABLE_LIFESPANS
    ) / len(CONSUMABLE_LIFESPANS)
    completion = max(
        0.05,
        min(1.0, random.gauss(0.88 - 0.25 * avg_wear, 0.08))
    )
    # Иногда — короткий «срыв» миссии (отмена/пауза)
    if random.random() < 0.05 + 0.10 * avg_wear:
        completion = round(random.uniform(0.02, 0.30), 3)
    completion = round(completion, 3)
    actual = round(planned * completion, 3)

    # Длительность пропорциональна площади + шум
    base_eff = random.uniform(280, 360)  # м^2/час
    if state.model_family == "W50":
        base_eff *= 1.3  # модели W50H быстрее
    duration_seconds = max(40, int(actual / base_eff * 3600))
    if completion < 0.3:
        duration_seconds = max(30, int(duration_seconds * random.uniform(0.3, 0.8)))

    end_dt = start_dt + timedelta(seconds=duration_seconds)
    efficiency = round(actual / max(duration_seconds, 1) * 3600, 3)
    water_consumption = round(actual * random.uniform(0.025, 0.035), 3)

    # Батарея: 1% ~ 30 кв.м.
    battery_drop = max(1, int(actual / 30) + random.randint(-2, 2))
    start_batt = state.last_battery_pct
    end_batt = max(5, start_batt - battery_drop)
    if end_batt < 25 and random.random() < 0.7:
        # Робот уходит на зарядку — следующая миссия с зарядки
        end_batt = random.randint(95, 100)
    state.last_battery_pct = end_batt

    # Износ расходников: 1 час эксплуатации = +1 к usedLife (в часах)
    delta_hours = duration_seconds / 3600.0
    for k in CONSUMABLE_LIFESPANS:
        state.used_life[k] += delta_hours

    # consumablesResidualPercentage в % от срока службы
    def residual_pct(name: str) -> int:
        used = state.used_life[name]
        life = CONSUMABLE_LIFESPANS[name]
        return max(0, int(round(100 - 100.0 * used / life)))

    residual = {
        "brush":        residual_pct("rollingBrush"),
        "filter":       residual_pct("cleanWaterFilter"),
        "suctionBlade": residual_pct("softSqueegee"),
    }

    map_id, map_name = MAP_NAMES[state.serial][0]
    cleaning_mode = random.choice(
        CLEAN_MODES_W1 if state.model_family == "W1" else CLEAN_MODES_W50H
    )

    task_id = str(uuid.uuid4())
    plan_id = str(uuid.uuid4())
    task_instance_id = str(uuid.uuid4())
    report_id = str(uuid.uuid4())

    task = {
        "id": report_id,
        "displayName": map_name,
        "robot": state.display_name,
        "robotSerialNumber": state.serial,
        "operator": operator,
        "completionPercentage": completion,
        "durationSeconds": duration_seconds,
        "areaNameList": map_name,
        "plannedCleaningAreaSquareMeter": planned,
        "actualCleaningAreaSquareMeter": actual,
        "efficiencySquareMeterPerHour": efficiency,
        "plannedPolishingAreaSquareMeter": 0,
        "actualPolishingAreaSquareMeter": 0,
        "waterConsumptionLiter": water_consumption,
        "startBatteryPercentage": start_batt,
        "endBatteryPercentage": end_batt,
        "consumablesResidualPercentage": residual,
        "taskInstanceId": task_instance_id,
        "cleaningMode": cleaning_mode,
        "taskEndStatus": 0 if completion >= 0.5 else random.choice([0, 1, 2]),
        "taskReportPngUri": f"https://api.cleanbot-cloud.ru/robot-task/report/png/v2/{report_id}",
        "subTasks": [
            {
                "mapId": map_id,
                "mapName": map_name,
                "actualCleaningAreaSquareMeter": actual,
                "taskId": task_id,
            }
        ],
        "taskId": task_id,
        "planId": plan_id,
        "planRunningTime": random.randint(800, 900),
        "startTime": iso(start_dt),
        "endTime": iso(end_dt),
    }

    # Обновляем эксплуатационные счётчики
    state.total_missions += 1
    state.total_runtime_seconds += duration_seconds
    state.total_area_m2 += actual

    return task, end_batt


# ---------------------------------------------------------------------------
# Генерация snapshot (robot_status)
# ---------------------------------------------------------------------------

# TaskSlot: (mission_start, mission_end, start_battery_pct, end_battery_pct)
TaskSlot = tuple[datetime, datetime, int, int]


def generate_status_snapshot(
    state: RobotState,
    snapshot_dt: datetime,
    *,
    battery_pct: int | None = None,
    task_state: str | None = None,
    charging: bool | None = None,
    speed_kmh: float = 0.0,
    clean_water_level: int | None = None,
    recovery_water_level: int | None = None,
    position: tuple[int, int, float] | None = None,
) -> dict[str, Any]:
    """
    Строит один снимок состояния робота.
    Именованные аргументы позволяют переопределить реальтайм-поля при
    использовании в generate_intraday_status_snapshots.
    """
    map_id, map_name = MAP_NAMES[state.serial][0]

    batt = battery_pct if battery_pct is not None else state.last_battery_pct
    is_charging = charging if charging is not None else (batt < 30 or random.random() < 0.3)
    t_state = task_state if task_state is not None else random.choice(
        ["IDLE", "PAUSED", "RUNNING", "CHARGING"]
    )

    cell_voltages = [random.randint(3580, 3640) for _ in range(7)]
    total_voltage = sum(cell_voltages) * 8 // 7

    if position is not None:
        pos_x, pos_y, angle = position
    else:
        pos_x = random.randint(100, 700)
        pos_y = random.randint(100, 700)
        angle = round(random.uniform(0.0, 359.999), 3)

    cw_level = clean_water_level if clean_water_level is not None else random.randint(40, 100)
    rw_level  = recovery_water_level if recovery_water_level is not None else random.randint(20, 80)

    snap = {
        "serialNumber": state.serial,
        "taskState": t_state,
        "online": True if t_state == "RUNNING" else random.random() < 0.85,
        "speedKilometerPerHour": round(speed_kmh, 2),
        "battery": {
            "charging": is_charging,
            "powerPercentage": batt,
            "chargerCurrent": random.randint(10, 20) if is_charging else 0,
            "totalVoltage": total_voltage,
            "current": random.randint(-80, -10) if t_state == "RUNNING" else
                       (random.randint(5, 20) if is_charging else 0),
            "fullCapacity": 40000,
            "soc": batt,
            "soh": state.battery_health,
            "cycleTimes": state.cycle_times,
            "protectorStatus": ["CELL_HIGH_VOLTAGE_PROTECTION"] if batt >= 99 else [],
            "temperature1": random.randint(28, 38) if t_state == "RUNNING" else random.randint(25, 32),
            "temperature2": random.randint(26, 36) if t_state == "RUNNING" else random.randint(24, 30),
            "temperature3": 0, "temperature4": 0, "temperature5": 0,
            "temperature6": 0, "temperature7": 0,
            "cellVoltage1": cell_voltages[0], "cellVoltage2": cell_voltages[1],
            "cellVoltage3": cell_voltages[2], "cellVoltage4": cell_voltages[3],
            "cellVoltage5": cell_voltages[4], "cellVoltage6": cell_voltages[5],
            "cellVoltage7": cell_voltages[6],
        },
        "emergencyStop": {"enabled": t_state != "RUNNING"},
        "localizationInfo": {
            "localizationState": "NORMAL",
            "map": {"id": map_id, "name": map_name, "version": ""},
            "mapPosition": {"x": pos_x, "y": pos_y, "angle": angle},
        },
        "cleanModes": [{"name": n} for n in (
            CLEAN_MODES_W1 if state.model_family == "W1" else CLEAN_MODES_W50H
        )],
        "supportSite": True,
        "navigationPoints": {
            "naviPoints": [
                {"naviPointName": "Origin",  "navPointGridX": 459, "navPointGridY": 509},
                {"naviPointName": "End",     "navPointGridX": 458, "navPointGridY": 514},
                {"naviPointName": "chrg",    "navPointGridX": 460, "navPointGridY": 504},
                {"naviPointName": "Current", "navPointGridX": pos_x, "navPointGridY": pos_y},
            ]
        },
        "workModes": [
            {"id": uuid.uuid4().hex, "name": n, "strength": "prefer_custom", "type": "1"}
            for n in (CLEAN_MODES_W1 if state.model_family == "W1" else CLEAN_MODES_W50H)
        ],
        "device": {
            "cleanWaterTank":     {"level": cw_level},
            "recoveryWaterTank":  {"level": rw_level},
            "rollingBrush": {
                "enabled": t_state == "RUNNING",
                "ifPutDown": t_state == "RUNNING",
                "lifeSpan": CONSUMABLE_LIFESPANS["rollingBrush"],
                "usedLife": round(state.used_life["rollingBrush"], 6),
                "motorFront": random.randint(-100, -10) if t_state == "RUNNING" else -1,
                "motorAfter":  random.randint(-100, -10) if t_state == "RUNNING" else 0,
            },
            "rightSideBrush": {
                "lifeSpan": CONSUMABLE_LIFESPANS["rightSideBrush"],
                "usedLife": round(state.used_life["rightSideBrush"], 6),
                "isRunning": t_state == "RUNNING",
            },
            "softSqueegee": {
                "ifPutDown": t_state == "RUNNING",
                "lifeSpan": CONSUMABLE_LIFESPANS["softSqueegee"],
                "usedLife": round(state.used_life["softSqueegee"], 6),
            },
            "rollingSqueegee": {"ifPutDown": False},
            "spray":  {"isRunning": t_state == "RUNNING", "waterLevel": cw_level if t_state == "RUNNING" else 0},
            "vacuum": {"enabled": t_state == "RUNNING", "level": random.randint(1, 3) if t_state == "RUNNING" else 0},
            "cleanWaterFilter": {
                "lifeSpan": CONSUMABLE_LIFESPANS["cleanWaterFilter"],
                "usedLife": round(state.used_life["cleanWaterFilter"], 6),
            },
            "hepaSensor": {
                "lifeSpan": CONSUMABLE_LIFESPANS["hepaSensor"],
                "usedLife": round(state.used_life["hepaSensor"], 6),
            },
        },
        "navStatus": "NAVI_RUNNING" if t_state == "RUNNING" else "NAVI_IDLE",
        "currentElevatorStatus": "ELEVATOR_CONTROLLER_IDLE",
        "latestReportTime": str(int(snapshot_dt.timestamp() * 1000)),
    }
    return snap


def generate_intraday_status_snapshots(
    state: RobotState,
    day: datetime,
    task_schedule: list[TaskSlot],
    load_dt: str,
    source: str,
    batch_id: str,
) -> list[dict[str, Any]]:
    """
    Генерирует временной ряд опросов статуса робота за один день.

    Каждые POLL_INTERVAL_MINUTES минут с POLL_START_HOUR до POLL_END_HOUR
    определяет состояние робота по расписанию задач и возвращает список
    словарей, каждый из которых содержит встроенные мета-поля Bronze
    (_poll_ts, _load_dt, _source, _batch_id, _schema) и пригоден для
    записи как одна строка JSONL.
    """
    schedule = sorted(task_schedule, key=lambda x: x[0])

    # Стартовый уровень батареи — от первой миссии, или от состояния робота
    battery = schedule[0][2] if schedule else state.last_battery_pct
    clean_water    = random.randint(85, 100)
    recovery_water = random.randint(5, 20)
    # Стартовая позиция — около дока/зарядки
    pos_x = random.randint(455, 470)
    pos_y = random.randint(500, 515)

    result: list[dict[str, Any]] = []
    poll_t = day.replace(hour=POLL_START_HOUR, minute=0, second=0, microsecond=0)
    poll_end = day.replace(hour=POLL_END_HOUR, minute=0, second=0, microsecond=0)

    while poll_t <= poll_end:
        # Найти активную задачу в текущий момент
        active: TaskSlot | None = None
        for slot in schedule:
            if slot[0] <= poll_t <= slot[1]:
                active = slot
                break

        if active is not None:
            m_start, m_end, sb, eb = active
            duration = max(1.0, (m_end - m_start).total_seconds())
            progress = (poll_t - m_start).total_seconds() / duration
            # Во время миссии батарея только тратится; если eb > sb,
            # зарядка произойдёт ПОСЛЕ миссии — до её конца не учитываем
            depleted = min(sb, eb) if eb < sb else max(5, sb - 20)
            battery = max(5, sb - int((sb - depleted) * progress))
            t_state = "RUNNING"
            speed = round(random.uniform(0.8, 2.8), 2)
            clean_water    = max(0,   clean_water    - WATER_CONSUME_PER_POLL)
            recovery_water = min(100, recovery_water + WATER_CONSUME_PER_POLL)
            # Случайное блуждание в пределах карты
            pos_x = max(100, min(700, pos_x + random.randint(-15, 15)))
            pos_y = max(100, min(700, pos_y + random.randint(-15, 15)))

        else:
            # Проверяем, нужна ли зарядка перед следующей миссией
            upcoming = [s for s in schedule if s[0] > poll_t]
            next_sb = upcoming[0][2] if upcoming else None

            needs_charge = battery < 25 or (next_sb is not None and next_sb > battery + 10)
            if needs_charge:
                t_state = "CHARGING"
                battery = min(100, battery + CHARGE_RATE_PER_POLL)
                # Возвращаемся к доку
                pos_x = max(100, min(700, pos_x + (460 - pos_x) // max(1, abs(460 - pos_x) // 5 + 1)))
                pos_y = max(100, min(700, pos_y + (505 - pos_y) // max(1, abs(505 - pos_y) // 5 + 1)))
                clean_water    = min(100, clean_water    + 1)
                recovery_water = max(0,   recovery_water - 1)
            else:
                t_state = random.choices(["IDLE", "PAUSED"], weights=[8, 2])[0]
                pos_x = max(100, min(700, pos_x + random.randint(-3, 3)))
                pos_y = max(100, min(700, pos_y + random.randint(-3, 3)))
            speed = 0.0

        snap = generate_status_snapshot(
            state, poll_t,
            battery_pct=battery,
            task_state=t_state,
            charging=(t_state == "CHARGING"),
            speed_kmh=speed if active is not None else 0.0,
            clean_water_level=clean_water,
            recovery_water_level=recovery_water,
            position=(pos_x, pos_y, round(random.uniform(0.0, 359.999), 3)),
        )
        # Встраиваем мета-поля прямо в запись — каждая строка JSONL самодостаточна
        snap["_poll_ts"]  = iso(poll_t)
        snap["_load_dt"]  = load_dt
        snap["_source"]   = source
        snap["_batch_id"] = batch_id
        snap["_schema"]   = BRONZE_SCHEMA_VERSION

        result.append(snap)
        poll_t += timedelta(minutes=POLL_INTERVAL_MINUTES)

    return result


# ---------------------------------------------------------------------------
# Генерация maintenance_events
# ---------------------------------------------------------------------------

def maybe_emit_maintenance(
    state: RobotState,
    day: datetime,
    events: list[dict[str, Any]],
    counter: list[int],
) -> None:
    """
    Решает, генерировать ли сегодня событие ТО для данного робота.
    Логика — нелинейная, зависит от износа и истории (см. п.9 в .md).
    """
    # 1. Плановое preventive — каждые ~PREVENTIVE_PLAN_DAYS дней
    if state.last_maintenance_date is None:
        days_since_pm = 9999
    else:
        days_since_pm = (day - state.last_maintenance_date).days

    # 2. Корректирующая: вероятность тем выше, чем выше износ
    avg_wear = sum(
        state.used_life[k] / CONSUMABLE_LIFESPANS[k]
        for k in CONSUMABLE_LIFESPANS
    ) / len(CONSUMABLE_LIFESPANS)

    corrective_prob = 0.001 + max(0.0, (avg_wear - 0.7)) * 0.25
    # дополнительный bump после 90+ дней без обслуживания
    if days_since_pm > 90:
        corrective_prob += 0.01

    # 3. Inspection без ремонта — редко
    inspection_prob = INSPECTION_PROB_PER_DAY

    decision = random.random()

    if days_since_pm >= PREVENTIVE_PLAN_DAYS and decision < 0.6:
        emit_event(state, day, "preventive", events, counter, failure_detected=False)
    elif decision < corrective_prob:
        emit_event(state, day, "corrective", events, counter, failure_detected=True)
    elif decision < corrective_prob + inspection_prob:
        emit_event(state, day, "inspection", events, counter, failure_detected=False)


def emit_event(
    state: RobotState,
    day: datetime,
    mtype: str,
    events: list[dict[str, Any]],
    counter: list[int],
    failure_detected: bool,
) -> None:
    """Сформировать запись maintenance_events, обновить состояние робота."""

    # Выбор компонента: для corrective — взвешенно по износу
    if mtype == "corrective":
        # Усиливаем вероятность пропорционально текущему износу компонента
        weighted = []
        for code, component, action, parts, base_w in FAILURE_CATALOG:
            wear_boost = 0.0
            if component == "brush":
                wear_boost = state.used_life["rollingBrush"] / CONSUMABLE_LIFESPANS["rollingBrush"]
            elif component == "filter":
                wear_boost = state.used_life["cleanWaterFilter"] / CONSUMABLE_LIFESPANS["cleanWaterFilter"]
            elif component == "suction_blade":
                wear_boost = state.used_life["softSqueegee"] / CONSUMABLE_LIFESPANS["softSqueegee"]
            elif component == "battery":
                wear_boost = min(1.0, state.cycle_times / 800.0)
            weighted.append(((code, component, action, parts), base_w * (1.0 + 2.0 * wear_boost)))
        code, component, action, parts = weighted_choice(weighted)
        category = "failure_repair"
        severity = random.choices(
            ["low", "medium", "high", "critical"],
            weights=[3, 5, 3, 1],
        )[0]
    elif mtype == "preventive":
        # Плановое — обслуживаем самый изношенный компонент
        worst = max(
            CONSUMABLE_LIFESPANS,
            key=lambda k: state.used_life[k] / CONSUMABLE_LIFESPANS[k],
        )
        worst_to_component = {
            "rollingBrush":     ("brush",         "brush_replacement",          ["brush"]),
            "rightSideBrush":   ("brush",         "side_brush_replacement",     ["brush"]),
            "softSqueegee":     ("suction_blade", "suction_blade_replacement",  ["suction_blade"]),
            "cleanWaterFilter": ("filter",        "filter_replacement",         ["filter"]),
            "hepaSensor":       ("filter",        "hepa_replacement",           ["filter"]),
        }
        component, action, parts = worst_to_component[worst]
        code = None
        category = "scheduled_service"
        severity = random.choice(["low", "medium"])
    else:  # inspection
        component = random.choice(["unknown", "battery", "lidar", "software"])
        action = "diagnostic"
        parts = []
        code = None
        category = "wear_related" if component in ("battery", "lidar") else "other"
        severity = "low"

    # Время
    reported_at  = day.replace(hour=random.randint(8, 11), minute=random.randint(0, 59))
    started_at   = reported_at + timedelta(minutes=random.randint(15, 180))
    if mtype == "preventive":
        duration = random.randint(45, 180)
    elif mtype == "inspection":
        duration = random.randint(20, 60)
    else:
        # corrective длительнее
        duration = random.randint(60, 360)
        if severity == "critical":
            duration = random.randint(240, 720)
    completed_at = started_at + timedelta(minutes=duration)
    downtime_minutes = duration

    counter[0] += 1
    event = {
        "maintenance_id":       f"mnt_{counter[0]:06d}",
        "robot_id":             state.serial,
        "service_ticket_id":    f"svc_{day.year}_{counter[0]:06d}",
        "service_source":       "synthetic",
        "maintenance_type":     mtype,
        "maintenance_category": category,
        "maintenance_status":   "completed",
        "reported_at":          iso(reported_at),
        "started_at":           iso(started_at),
        "completed_at":         iso(completed_at),
        "severity":             severity,
        "failure_detected":     failure_detected,
        "failure_code":         code,
        "component":            component,
        "action_taken":         action,
        "parts_replaced":       parts,
        "downtime_minutes":     downtime_minutes,
        "technician_id":        f"tech_{random.randint(1, 8):03d}",
        "comment":              None,
        "created_at":           iso(reported_at),
        "updated_at":           iso(completed_at),
        "dt":                   completed_at.date().isoformat(),
    }
    events.append(event)

    # Эффект ТО на состояние робота
    state.last_maintenance_date = completed_at
    if failure_detected:
        state.last_failure_date = completed_at

    # Сброс ресурсов заменённых деталей
    component_to_consumables = {
        "brush":         ["rollingBrush", "rightSideBrush"],
        "filter":        ["cleanWaterFilter", "hepaSensor"],
        "suction_blade": ["softSqueegee"],
    }
    for cons in component_to_consumables.get(component, []):
        state.used_life[cons] = 0.0
    if mtype == "preventive":
        # Плановое ТО — символический сброс «всех» расходников частично
        for cons in CONSUMABLE_LIFESPANS:
            state.used_life[cons] *= 0.3
    if component == "battery" and failure_detected:
        state.battery_health = "AGING"


# ---------------------------------------------------------------------------
# Основной цикл симуляции
# ---------------------------------------------------------------------------

def simulate(days: int, out_dir: Path, seed: int) -> None:
    random.seed(seed)
    end_date = datetime(2026, 5, 1, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=days)

    # 1) Подготовить состояние роботов
    states: dict[str, RobotState] = {}
    for r in ROBOT_FLEET:
        s = RobotState(
            serial=r["serialNumber"],
            display_name=r["displayName"],
            model_family=r["modelFamilyCode"],
            model_type=r["modelTypeCode"],
            software_version=r["softwareVersion"],
            hardware_version=r["hardwareVersion"],
        )
        # Лёгкая стартовая «биография» — у разных роботов разный износ
        for k in CONSUMABLE_LIFESPANS:
            s.used_life[k] = random.uniform(0, CONSUMABLE_LIFESPANS[k] * 0.4)
        s.cycle_times = random.randint(20, 200)
        s.last_battery_pct = random.randint(60, 100)
        states[s.serial] = s

    # 2) Симуляция: день за днём.
    #    Складываем результаты сразу по дням-партициям (load_dt = день события),
    #    чтобы сразу класть их в Bronze-структуру source=…/load_dt=…
    tasks_by_day:    dict[str, list[dict[str, Any]]] = {}
    status_by_day:   dict[str, list[dict[str, Any]]] = {}
    maint_by_day:    dict[str, list[dict[str, Any]]] = {}
    list_robots_by_day: dict[str, list[dict[str, Any]]] = {}

    counter = [0]  # mutable для emit_event
    batch_id = uuid.uuid4().hex[:12]

    cur = start_date
    while cur <= end_date:
        load_dt = cur.date().isoformat()

        # 2a) Snapshot реестра роботов на день — поле online варьируется
        list_robots_by_day[load_dt] = [
            {
                "serialNumber":    s.serial,
                "name":            f"robots/{s.serial}",
                "displayName":     s.display_name,
                "modelFamilyCode": s.model_family,
                "modelTypeCode":   s.model_type,
                "online":          random.random() < 0.4,
                "softwareVersion": s.software_version,
                "hardwareVersion": s.hardware_version,
            }
            for s in states.values()
        ]

        # 2b) События по каждому роботу за день
        for state in states.values():
            day_maint: list[dict[str, Any]] = []
            maybe_emit_maintenance(state, cur, day_maint, counter)
            if day_maint:
                maint_by_day.setdefault(load_dt, []).extend(day_maint)

            # Если сегодня было ТО — нагрузка ниже
            today_load_factor = 1.0
            if (state.last_maintenance_date
                    and (cur - state.last_maintenance_date).days == 0):
                today_load_factor = 0.4

            lo, hi = SITE_PROFILES[state.serial]["missions_per_day"]
            missions_today = int(round(random.randint(lo, hi) * today_load_factor))

            t = cur.replace(
                hour=random.randint(7, 9),
                minute=random.randint(0, 59),
                second=0, microsecond=0,
            )
            daily_schedule: list[TaskSlot] = []
            for _ in range(missions_today):
                t += timedelta(minutes=random.randint(5, 60))
                operator = random.choice(OPERATORS)
                task, _ = generate_task(state, t, operator)
                tasks_by_day.setdefault(load_dt, []).append(task)
                # Запоминаем окно миссии для intraday-опроса
                m_start = datetime.fromisoformat(task["startTime"].replace("Z", "+00:00"))
                m_end   = datetime.fromisoformat(task["endTime"].replace("Z", "+00:00"))
                daily_schedule.append((
                    m_start, m_end,
                    task["startBatteryPercentage"],
                    task["endBatteryPercentage"],
                ))
                t += timedelta(seconds=task["durationSeconds"])

            # Временной ряд опросов статуса за день (один снимок каждые 15 мин)
            intraday = generate_intraday_status_snapshots(
                state, cur, daily_schedule, load_dt, "gausium", batch_id,
            )
            status_by_day.setdefault(load_dt, []).extend(intraday)

            if random.random() < 0.4:
                state.cycle_times += 1

        cur += timedelta(days=1)

    # 3) Запись Bronze-партиций.
    #    Layout:
    #      bronze/<dataset>/source=<source>/load_dt=<date>/<file>.json
    #
    #    Конверт каждого файла: { "_meta": {...}, "data": <raw> }

    # 3a) list_robots — snapshot реестра флота, один файл на день
    for load_dt, robots in list_robots_by_day.items():
        # Форма API: список из одной «страницы» с массивом roboтов
        raw_payload = [{
            "robots":   robots,
            "page":     1,
            "pageSize": len(robots),
            "total":    str(len(robots)),
        }]
        write_bronze(
            out_dir, "list_robots", "gausium", load_dt,
            "list_robots.json", raw_payload, batch_id=batch_id,
        )

    # 3b) robot_status — JSONL, один файл на партицию.
    #     Каждая строка = один опрос (каждые POLL_INTERVAL_MINUTES минут).
    #     Мета-поля _poll_ts, _load_dt, _source, _batch_id, _schema уже встроены
    #     в каждую запись функцией generate_intraday_status_snapshots.
    for load_dt, snaps in status_by_day.items():
        write_bronze_jsonl(
            out_dir, "robot_status", "gausium", load_dt,
            "robot_status.jsonl", snaps,
        )

    # 3c) task_reports — обёртка как у API, по странице за день
    for load_dt, tasks in tasks_by_day.items():
        raw_payload = [{
            "robotTaskReports": tasks,
            "page":             1,
            "pageSize":         len(tasks),
            "total":            str(len(tasks)),
        }]
        write_bronze(
            out_dir, "task_reports", "gausium", load_dt,
            "task_reports.json", raw_payload, batch_id=batch_id,
        )

    # 3d) maintenance_events — синтетический источник, файл только на дни
    #     с событиями (типично для daily batch-выгрузки)
    total_maint = 0
    for load_dt, events in maint_by_day.items():
        total_maint += len(events)
        write_bronze(
            out_dir, "maintenance_events", "synthetic", load_dt,
            "maintenance_events.json", events, batch_id=batch_id,
        )

    # 4) Краткий отчёт
    total_tasks  = sum(len(v) for v in tasks_by_day.values())
    total_snaps  = sum(len(v) for v in status_by_day.values())
    polls_per_day = (POLL_END_HOUR - POLL_START_HOUR) * 60 // POLL_INTERVAL_MINUTES + 1
    by_type: dict[str, int] = {}
    for events in maint_by_day.values():
        for e in events:
            by_type[e["maintenance_type"]] = by_type.get(e["maintenance_type"], 0) + 1

    print("=== Synthetic generation complete ===")
    print(f"Period:              {start_date.date()} … {end_date.date()}  ({days} days, batch_id={batch_id})")
    print(f"Robots:              {len(states)}")
    print(f"Task reports:        {total_tasks}  ({len(tasks_by_day)} load_dt partitions)")
    print(f"Status polls:        {total_snaps}  ({len(status_by_day)} load_dt partitions,")
    print(f"                     ~{polls_per_day} polls/robot/day × {len(states)} robots,")
    print(f"                     interval={POLL_INTERVAL_MINUTES} min, {POLL_START_HOUR}:00–{POLL_END_HOUR}:00 UTC)")
    print(f"Maintenance events:  {total_maint}  ({len(maint_by_day)} load_dt partitions)")
    print(f"  by type: {by_type}")
    print(f"List robots snaps:   {len(list_robots_by_day)} load_dt partitions")
    print(f"Output dir:          {(out_dir / 'bronze').resolve()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Synthetic Gausium + maintenance data generator")
    p.add_argument("--days", type=int, default=180, help="Период симуляции в днях (по умолчанию 180)")
    p.add_argument("--out",  type=str, default="./data", help="Каталог для вывода (по умолчанию ./data)")
    p.add_argument("--seed", type=int, default=42, help="Seed для воспроизводимости")
    args = p.parse_args()
    simulate(args.days, Path(args.out), args.seed)


if __name__ == "__main__":
    main()
