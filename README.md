# Robopulse — Predictive Maintenance Pipeline

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

## Стек

| Компонент | Технология | Назначение |
|---|---|---|
| Оркестрация | Apache Airflow 2.9 | Планирование и мониторинг задач |
| Обработка данных | Apache Spark 3.5 (PySpark) | Трансформации Bronze → Silver → Gold |
| Хранилище | MinIO (S3-совместимое) | Хранение всех слоёв данных |
| Контейнеризация | Docker / Docker Compose | Единая среда запуска всех сервисов |
| Генерация данных | Python (generate_data.py) | Синтетические Bronze-данные |
| ML-модель | scikit-learn (Jupyter) | Gradient Boosting, ROC-AUC 0.923 |

---

## Структура репозитория

```
robopulse/
├── docker-compose.yml          # Все сервисы: Airflow, Spark, MinIO, Postgres
├── .env                        # Credentials (не коммитится)
├── Makefile                    # Удобные команды (up, down, trigger, logs, ...)
│
├── airflow/
│   ├── Dockerfile              # Airflow + Java + pyspark + boto3
│   └── dags/
│       └── robopulse_dag.py    # DAG: Bronze upload → Silver → Gold
│
├── spark/
│   ├── Dockerfile              # apache/spark:3.5.5 + hadoop-aws JARs
│   └── jobs/
│       ├── silver_job.py       # PySpark: Bronze (JSON/JSONL) → Silver (Parquet)
│       └── gold_job.py         # PySpark: Silver → Gold feature dataset
│
├── generate_data.py            # Генератор синтетических Bronze-данных
├── process_silver.py           # Bronze → Silver на DuckDB (локальный вариант)
│
└── jupiter/
    ├── eda.ipynb               # EDA: анализ Bronze/Silver
    └── ml.ipynb                # Gold layer + ML-модель
```

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────┐  │
│  │   Airflow    │    │    Spark     │    │      MinIO        │  │
│  │  Webserver   │    │   Master     │    │  (S3-совместимый) │  │
│  │  Scheduler   │───▶│   Worker     │───▶│  s3://robopulse/  │  │
│  │ (LocalExec.) │    │  (6G / 4CPU) │    │  bronze/          │  │
│  └──────────────┘    └──────────────┘    │  silver/          │  │
│         │                               │  gold/            │  │
│  ┌──────┴───────┐                       └───────────────────┘  │
│  │  PostgreSQL  │                                               │
│  │ (Airflow DB) │                                               │
│  └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

**Поток данных:**
1. `generate_and_upload_bronze` — Airflow PythonOperator генерирует синтетические JSON/JSONL и загружает их в MinIO (`s3://robopulse/bronze/`)
2. `process_silver` — SparkSubmitOperator запускает `silver_job.py`: нормализует Bronze в типизированный Parquet (`s3://robopulse/silver/`)
3. `process_gold` — SparkSubmitOperator запускает `gold_job.py`: строит feature dataset для ML (`s3://robopulse/gold/`)

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
s3://robopulse/bronze/
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
Реализована на **PySpark** (`spark/jobs/silver_job.py`).

```
s3://robopulse/silver/
├── maintenance_events/dt=YYYY-MM-DD/*.parquet   (93 записи,    67 партиций,  ~217 КБ)
├── task_reports/dt=YYYY-MM-DD/*.parquet         (11 095 записей, 182 партиции, ~2 МБ)
└── robot_status/dt=YYYY-MM-DD/*.parquet         (149 868 записей, 181 партиция, ~3.5 МБ)
```

Сжатие относительно Bronze: **98%** (379 МБ → ~5.7 МБ Parquet).

---

### Gold

Feature dataset для ML — одна строка на `(robot, feature_date)`.  
Реализован на **Spark SQL** (`spark/jobs/gold_job.py`).

```
s3://robopulse/gold/robot_features/part-*.snappy.parquet   (216 строк × 23 колонки)
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
Код и визуализации: `jupiter/ml.ipynb`.

| Метрика | Logistic Regression | **Gradient Boosting** |
|---|---|---|
| ROC-AUC (test) | 0.908 | **0.923** |
| Avg Precision (test) | 0.905 | **0.926** |
| CV ROC-AUC (5-fold) | 0.992 ± 0.012 | 0.984 ± 0.011 |

При оптимальном пороге 0.64: Recall = **100%** (23/23 отказов поймано),  
чистая экономия простоя на тестовом периоде — **83.6 часов**.

---

## Требования к серверу

| Параметр | Минимум | Рекомендовано |
|---|---|---|
| CPU | 4 ядра | **8 ядер** |
| RAM | 16 GB | **32 GB** |
| Диск | 100 GB SSD | **200 GB SSD** |
| ОС | Ubuntu 22.04+ | Ubuntu 24.04 LTS |

---

## Запуск на чистой машине

### 1. Установка Docker (Ubuntu 22.04 / 24.04)

```bash
# Добавляем официальный репозиторий Docker
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin

# Разрешаем запускать Docker без sudo (требует перелогина)
sudo usermod -aG docker $USER
```

### 2. Клонирование репозитория

```bash
git clone <repo-url> robopulse
cd robopulse
```

### 3. Сборка и запуск сервисов

> Первый запуск загружает образы и компилирует зависимости — занимает **5–10 минут**.

```bash
# Собрать образы и запустить все сервисы
sudo docker compose up -d --build
```

Что происходит:
- **postgres** — поднимается первым, ждёт `pg_isready`
- **minio** — создаёт bucket `robopulse`
- **spark-master / spark-worker** — Spark кластер (master + 1 worker, 6G RAM / 4 CPU)
- **airflow-init** — мигрирует БД, создаёт пользователя `admin`
- **airflow-webserver / airflow-scheduler** — стартуют после успешного init

Проверить состояние:

```bash
sudo docker compose ps
```

Все сервисы должны быть `healthy` или `running`.

### 4. Запуск пайплайна

```bash
# Снять DAG с паузы и запустить
sudo docker exec robopulse-airflow-scheduler-1 \
  airflow dags unpause robopulse_pipeline

sudo docker exec robopulse-airflow-scheduler-1 \
  airflow dags trigger robopulse_pipeline
```

Или через веб-интерфейс Airflow (см. адреса ниже): **DAGs → robopulse_pipeline → Trigger**.

### 5. Мониторинг выполнения

```bash
# Статус последнего запуска (выполнять периодически)
sudo docker exec robopulse-airflow-scheduler-1 \
  airflow dags list-runs -d robopulse_pipeline

# Логи планировщика в реальном времени
sudo docker compose logs -f airflow-scheduler
```

Ожидаемое время выполнения на рекомендуемом железе:

| Задача | Время |
|---|---|
| `generate_and_upload_bronze` | ~30 сек |
| `process_silver` | ~50 сек |
| `process_gold` | ~25 сек |
| **Итого** | **~2 мин** |

### 6. Проверка результатов в MinIO

После успешного завершения данные доступны в веб-консоли MinIO  
(`http://<IP>:9001`, вкладка **Object Browser → robopulse**):

```
robopulse/
├── bronze/   — ~379 МБ JSON/JSONL (869 файлов)
├── silver/   — ~5.7 МБ Parquet, партиционировано по dt
└── gold/     — ~22 КБ, один Parquet-файл
```

---

## Веб-интерфейсы

| Сервис | Адрес | Логин / Пароль |
|---|---|---|
| **Airflow** | `http://<IP>:8081` | admin / admin |
| **MinIO Console** | `http://<IP>:9001` | minioadmin / minioadmin123 |
| **Spark UI** | `http://<IP>:8080` | — |

---

## Makefile — быстрые команды

```bash
make up          # Собрать образы и запустить все сервисы
make down        # Остановить контейнеры (данные сохраняются)
make logs        # Логи всех сервисов в реальном времени
make logs-airflow-scheduler   # Логи конкретного сервиса
make trigger     # Запустить DAG вручную
make status      # Статус последнего запуска DAG
make reset       # Полный сброс: удалить контейнеры и volumes
```

---

## Чтение данных из MinIO

### PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("robopulse-read") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://<IP>:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Silver task_reports
tasks = spark.read.parquet("s3a://robopulse/silver/task_reports")

# Gold feature dataset
gold = spark.read.parquet("s3a://robopulse/gold/robot_features")
gold.show(5)
```

### boto3 / pandas (через MinIO S3 API)

```python
import boto3, pandas as pd
from io import BytesIO

s3 = boto3.client(
    "s3",
    endpoint_url="http://<IP>:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
)

# Gold feature dataset
obj = s3.get_object(Bucket="robopulse", Key="gold/robot_features/part-00000-*.snappy.parquet")
gold = pd.read_parquet(BytesIO(obj["Body"].read()))
print(gold.shape)  # (216, 23)
```
