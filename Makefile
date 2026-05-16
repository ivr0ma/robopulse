.PHONY: up down build logs reset trigger status ui

COMPOSE := docker compose -f infra/docker-compose.yml -p robopulse

## Запуск (сборка образов при первом запуске)
up:
	$(COMPOSE) up -d --build

## Остановка контейнеров (данные сохраняются)
down:
	$(COMPOSE) down

## Пересборка образов без кэша
build:
	$(COMPOSE) build --no-cache

## Логи всех сервисов (Ctrl+C для выхода)
logs:
	$(COMPOSE) logs -f

## Логи конкретного сервиса: make logs-airflow-scheduler
logs-%:
	$(COMPOSE) logs -f $*

## Полный сброс: удалить контейнеры + volumes (данные MinIO и Postgres будут потеряны!)
reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d --build

## Ручной запуск DAG
trigger:
	$(COMPOSE) exec airflow-scheduler \
		airflow dags trigger robopulse_pipeline

## Статус последнего запуска DAG
status:
	$(COMPOSE) exec airflow-scheduler \
		airflow dags list-runs -d robopulse_pipeline --no-backfill

## Адреса UI
ui:
	@echo "Airflow  : http://$(SERVER_IP):8081  (admin / admin)"
	@echo "MinIO    : http://$(SERVER_IP):9001  (minioadmin / minioadmin123)"
	@echo "Spark    : http://$(SERVER_IP):8080"
