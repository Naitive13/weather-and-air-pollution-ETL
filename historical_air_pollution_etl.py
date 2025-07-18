import sys
import os

from dags.weather_and_air_pollution.scripts.extract_historical_air_pollution import (
    get_historical_air_pollution,
)

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Configuration par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 4),
}

with DAG(
    "historical_air_pollution_etl",
    default_args=default_args,
    schedule="@daily",  # Exécution quotidienne
    catchup=False,  # Ne pas rattraper les exécutions passées
    max_active_runs=1,  # Pour éviter les conflits
) as dag:
    extract_historical_data_seoul = PythonOperator(
        task_id="extract_historical_air_pollution_data_seoul",
        python_callable=get_historical_air_pollution,
        op_args=["Seoul", "{{ var.value.API_KEY }}"],
        # ds = date de l'exécution au format YYYY-MM-DD
    )
    extract_historical_data_san_fransisco = PythonOperator(
        task_id="extract_historical_air_pollution_data_san_fransisco",
        python_callable=get_historical_air_pollution,
        op_args=["San Fransisco", "{{ var.value.API_KEY }}"],
        # ds = date de l'exécution au format YYYY-MM-DD
    )
    extract_historical_data_antananarivo = PythonOperator(
        task_id="extract_historical_air_pollution_data_antananarivo",
        python_callable=get_historical_air_pollution,
        op_args=["Antananarivo", "{{ var.value.API_KEY }}"],
        # ds = date de l'exécution au format YYYY-MM-DD
    )

    # ========== Orchestration ==========
    # Les tâches d'extraction s'exécutent en parallèle
    # puis la fusion s'exécute, suivie de la transformation
    # extract_air_pollution_data_tasks >> merge_air_pollution_data_task
    (
        extract_historical_data_seoul
        >> extract_historical_data_san_fransisco
        >> extract_historical_data_antananarivo
    )
