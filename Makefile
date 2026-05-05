.PHONY: up down build logs reset trigger status

## Запуск (сборка образов при первом запуске)
up:
	docker compose up -d --build

## Остановка контейнеров (данные сохраняются)
down:
	docker compose down

## Пересборка образов без кэша
build:
	docker compose build --no-cache

## Логи всех сервисов (Ctrl+C для выхода)
logs:
	docker compose logs -f

## Логи конкретного сервиса: make logs-airflow-scheduler
logs-%:
	docker compose logs -f $*

## Полный сброс: удалить контейнеры + volumes (данные MinIO и Postgres будут потеряны!)
reset:
	docker compose down -v
	docker compose up -d --build

## Ручной запуск DAG
trigger:
	docker compose exec airflow-scheduler \
		airflow dags trigger robopulse_pipeline

## Статус последнего запуска DAG
status:
	docker compose exec airflow-scheduler \
		airflow dags list-runs -d robopulse_pipeline --no-backfill

## Адреса UI
ui:
	@echo "Airflow  : http://$(SERVER_IP):8081  (admin / admin)"
	@echo "MinIO    : http://$(SERVER_IP):9001  (minioadmin / minioadmin123)"
	@echo "Spark    : http://$(SERVER_IP):8080"
