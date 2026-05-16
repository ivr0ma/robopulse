from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common import DEFAULT_ARGS, PARTITION_DT


with DAG(
    dag_id="robopulse_pipeline",
    description="Управляющий DAG Robopulse: Source -> Silver -> Gold",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 11),
    schedule_interval="@daily",
    catchup=True,
    tags=["robopulse", "orchestration"],
) as dag:
    run_source = TriggerDagRunOperator(
        task_id="run_source_dag",
        trigger_dag_id="robopulse_source",
        conf={"partition_dt": PARTITION_DT},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_silver = TriggerDagRunOperator(
        task_id="run_silver_dag",
        trigger_dag_id="robopulse_silver",
        conf={"partition_dt": PARTITION_DT},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_gold = TriggerDagRunOperator(
        task_id="run_gold_dag",
        trigger_dag_id="robopulse_gold",
        conf={"partition_dt": PARTITION_DT},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    run_source >> run_silver >> run_gold
