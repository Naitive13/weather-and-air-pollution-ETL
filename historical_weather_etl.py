import sys
import os

from dags.weather_and_air_pollution.scripts.transform_weather_history import (
    transform_historical_weather_data,
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
    "historical_weather_etl",
    default_args=default_args,
    schedule="@daily",  # Exécution quotidienne
    catchup=False,  # Ne pas rattraper les exécutions passées
    max_active_runs=1,  # Pour éviter les conflits
) as dag:
    transform_historical_data_seoul = PythonOperator(
        task_id="transform_historical_weather_data_seoul",
        python_callable=transform_historical_weather_data,
        op_args=["Seoul"],
        # ds = date de l'exécution au format YYYY-MM-DD
    )
    transform_historical_data_san_fransisco = PythonOperator(
        task_id="transform_historical_weather_data_san_fransisco",
        python_callable=transform_historical_weather_data,
        op_args=["San Fransisco"],
        # ds = date de l'exécution au format YYYY-MM-DD
    )
    transform_historical_data_antananarivo = PythonOperator(
        task_id="transform_historical_weather_data_antananarivo",
        python_callable=transform_historical_weather_data,
        op_args=["Antananarivo"],
        # ds = date de l'exécution au format YYYY-MM-DD
    )
    # ========== Orchestration ==========
    # Les tâches d'extraction s'exécutent en parallèle
    # puis la fusion s'exécute, suivie de la transformation
    # extract_air_pollution_data_tasks >> merge_air_pollution_data_task
    (
        transform_historical_data_seoul
        >> transform_historical_data_san_fransisco
        >> transform_historical_data_antananarivo
    )
