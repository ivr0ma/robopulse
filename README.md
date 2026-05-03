# Robopulse — Predictive Maintenance Pipeline (MVP)

End-to-end pipeline предиктивного обслуживания роботов-уборщиков:  
синтетическая генерация данных → Bronze → Silver → Gold → ML-модель.

Данные воспроизводят структуру реального облачного API роботов и схему событий  
технического обслуживания. Реальные идентификаторы клиентов, локаций и роботов заменены  
на вымышленные.

---

## Контекст

Проект является частью ВКР по теме **«Разработка системы предиктивного технического обслуживания роботов»**.

Основной объект исследования — pipeline извлечения, нормализации и batch-расчёта признаков  
на данных о работе роботов-уборщиков. ML-модель предсказывает вероятность отказа  
в горизонте 30 дней на основе накопленных признаков эксплуатации и истории обслуживания.

Синтетический источник `maintenance_events` в дальнейшем заменяется интеграцией с Bitrix  
при сохранении схемы Silver-слоя и логики расчёта признаков.

---

## Структура репозитория

```
robopulse/
├── generate_data.py        # Генератор Bronze-данных (оба источника)
├── process_silver.py       # Bronze → Silver нормализация (DuckDB)
│
├── jupiter/
│   ├── eda.ipynb           # Исследовательский анализ данных (EDA)
│   └── ml.ipynb            # Gold layer + ML-модель
│
└── data/                   # Генерируется локально, в git не включается
    ├── bronze/             # Сырые данные в Hive-партициях (JSON / JSONL)
    ├── silver/             # Нормализованные данные (Parquet)
    └── gold/               # Feature dataset для ML (Parquet)
```

---

## Источники данных

### Gausium-compatible API (роботы)

| Ресурс | Описание | Частота |
|---|---|---|
| `list_robots` | Реестр флота: серийный номер, модель, версия ПО | 1 снапшот/день |
| `robot_status` | Батарея, расходники, позиция, состояние задачи | каждые 15 мин, 06:00–23:00 UTC |
| `task_reports` | Отчёт о миссии: площадь, расход воды, батарея, расходники | ежедневный батч |

### maintenance_events (синтетика / Bitrix)

События ТО: `preventive`, `corrective`, `inspection`.  
Вероятность события зависит от накопленного износа расходников:

```
накопленный износ → вероятность ТО → тип события → компонент → простой
```

После события ТО счётчик `usedLife` расходника сбрасывается — это отражается  
в последующих снапшотах `robot_status` и полях `consumablesResidualPercentage` отчётов.

---

## Флот роботов

12 роботов двух модельных линеек на 9 условных объектах:

| Серийный номер | Объект | Модель |
|---|---|---|
| CB500-2100-W1R-0001 | БЦ Орбита, 1 эт., вход | CleanBot W1 |
| CB500-2100-W1R-0002 | БЦ Орбита, 1 эт., кафетерий | CleanBot W1 |
| CB500-2100-W1R-0003 | БЦ Орбита, 1 эт., спортзал | CleanBot W1 |
| CB500-2100-W1R-0004 | БЦ Орбита, 3 эт. | CleanBot W1 |
| CB500-2100-W1R-0005 | Гипермаркет Полярис | CleanBot W1 |
| CB500-2100-W1R-0006 | ТЦ Галактика Юг | CleanBot W1 |
| CB500-2100-W1R-0007 | ТЦ Галактика Центр | CleanBot W1 |
| CB500-2100-W1R-0008 | БЦ Энергия | CleanBot W1 |
| CB500-2100-W1R-0009 | ТЦ Меридиан | CleanBot W1 |
| CB500-2100-W1R-0010 | Магазин Орион ЕКБ | CleanBot W1 |
| CB300-2200-W5R-0001 | Отель Астра | CleanBot W50H |
| CB300-2200-W5R-0002 | Банк Кассиопея | CleanBot W50H |

---

## Pipeline

### Bronze

Hive-style партиционирование `source=…/load_dt=YYYY-MM-DD/`.  
Данные хранятся в сыром виде — без нормализации типов, в исходной схеме API.

```
data/bronze/
├── list_robots/source=gausium/load_dt=YYYY-MM-DD/list_robots.json
├── robot_status/source=gausium/load_dt=YYYY-MM-DD/robot_status.jsonl
├── task_reports/source=gausium/load_dt=YYYY-MM-DD/task_reports.json
└── maintenance_events/source=synthetic/load_dt=YYYY-MM-DD/maintenance_events.json
```

Каждый JSON-файл обёрнут в стандартный envelope:

```json
{
  "_meta": { "source": "gausium", "load_dt": "2025-11-18",
             "ingest_ts": "...", "batch_id": "...", "schema": "bronze.v1" },
  "data": [ ... ]
}
```

`robot_status.jsonl` — JSONL без envelope: мета-поля `_poll_ts`, `_source`, `_batch_id`  
встроены в каждую строку. 1 строка = 1 опрос (все роботы в один момент времени).

| Датасет | Записей | Партиций | Размер |
|---|---|---|---|
| `list_robots` | 181 снапшот | 181 | ~2 МБ |
| `robot_status` | 149 868 опросов | 181 | ~366 МБ |
| `task_reports` | 11 095 миссий | 181 | ~11 МБ |
| `maintenance_events` | 93 события | 67 | <1 МБ |
| **Bronze итого** | | | **~379 МБ** |

---

### Silver

Типизированные Parquet-партиции `dt=YYYY-MM-DD/`.  
Нормализация: типы, snake_case имена, дедупликация, выравнивание вложенных структур.

```
data/silver/
├── maintenance_events/dt=YYYY-MM-DD/*.parquet   (93 записи,   67 партиций,  ~217 КБ)
├── task_reports/dt=YYYY-MM-DD/*.parquet         (11 095 записей, 182 партиции, ~2 МБ)
└── robot_status/dt=YYYY-MM-DD/*.parquet         (149 868 записей, 181 партиция,  ~3.5 МБ)
```

Сжатие относительно Bronze: **98%** (379 МБ → ~5.7 МБ Parquet).

---

### Gold

Feature dataset для ML — одна строка на `(robot, feature_date)`.

```
data/gold/robot_features.parquet   (216 строк × 20 признаков)
```

| Группа | Признаки |
|---|---|
| Износ расходников | `brush/filter/squeegee_residual_last` |
| Активность | `missions_7d/30d`, `area_30d/90d` |
| Качество миссий | `avg_completion_7d/30d`, `avg_battery_drop_30d` |
| История ТО | `days_since_last_maintenance/failure`, `maint/failure_count`, `downtime_30d` |
| Батарея | `battery_cycle_times_last`, `avg_battery_soc_7d`, `battery_aging` |
| **Целевая** | `has_failure_next_30d` — отказ в следующие 30 дней |

---

### ML-модель

Gradient Boosting Classifier, темпоральный train/test сплит.

| Метрика | Logistic Regression | **Gradient Boosting** |
|---|---|---|
| ROC-AUC (test) | 0.908 | **0.923** |
| Avg Precision (test) | 0.905 | **0.926** |
| CV ROC-AUC (5-fold) | 0.992 ± 0.012 | 0.984 ± 0.011 |

При оптимальном пороге 0.64: Recall = **100%** (23/23 отказов поймано),  
чистая экономия простоя на тестовом периоде — **83.6 часов**.

---

## Запуск

### 1. Установка зависимостей

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

pip install duckdb scikit-learn pandas pyarrow matplotlib seaborn jupyter
```

### 2. Генерация Bronze-данных

```bash
# Базовый прогон (180 дней, seed=42)
python3 generate_data.py

# Явные параметры
python3 generate_data.py --days 180 --out ./data --seed 42
```

| Аргумент | По умолчанию | Описание |
|---|---|---|
| `--days` | `180` | Длина периода симуляции в днях |
| `--out` | `./data` | Корневой каталог вывода |
| `--seed` | `42` | Seed для воспроизводимости |

### 3. Построение Silver-слоя

```bash
python3 process_silver.py
# или с явными путями:
python3 process_silver.py --bronze ./data/bronze --silver ./data/silver
```

### 4. Запуск ноутбуков

> Ноутбуки используют относительные пути вида `data/silver/...`,  
> поэтому Jupyter необходимо запускать из **корня репозитория**.

```bash
jupyter notebook jupiter/
```

| Ноутбук | Описание |
|---|---|
| `jupiter/eda.ipynb` | EDA: анализ Bronze/Silver, корреляции, динамика износа |
| `jupiter/ml.ipynb` | Gold layer + обучение модели + оценка бизнес-эффекта |

---

## Чтение данных

### Bronze (Python)

```python
import json, glob

# Миссии за конкретный день
day = json.load(open("data/bronze/task_reports/source=gausium/load_dt=2026-04-30/task_reports.json"))
missions = day["data"][0]["robotTaskReports"]

# Временной ряд статусов одного робота за день
serial = "CB500-2100-W1R-0001"
rows = [json.loads(l) for l in
        open("data/bronze/robot_status/source=gausium/load_dt=2025-11-18/robot_status.jsonl")
        if json.loads(l)["serialNumber"] == serial]

# Все события ТО
events = []
for path in sorted(glob.glob("data/bronze/maintenance_events/source=synthetic/load_dt=*/maintenance_events.json")):
    events.extend(json.load(open(path))["data"])
```

### Silver / Gold (DuckDB или pandas)

```python
import duckdb

con = duckdb.connect()

# Silver task_reports
tasks = con.execute(
    "SELECT * FROM read_parquet('data/silver/task_reports/dt=*/*.parquet')"
).df()

# Silver robot_status — конкретный робот за один день
status = con.execute("""
    SELECT poll_ts, task_state, battery_soc, rolling_brush_used_life_h
    FROM read_parquet('data/silver/robot_status/dt=*/*.parquet')
    WHERE serial_number = 'CB500-2100-W1R-0001' AND dt = '2025-11-18'
    ORDER BY poll_ts
""").df()

# Gold feature dataset
gold = con.execute(
    "SELECT * FROM read_parquet('data/gold/robot_features.parquet')"
).df()
```
