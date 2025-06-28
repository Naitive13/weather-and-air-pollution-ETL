import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


from dags.weather_and_air_pollution.scripts.extract_weather import (
    get_current_weather_data,
)
from dags.weather_and_air_pollution.scripts.extract_air_pollution import (
    get_current_air_pollution_data,
)
from dags.weather_and_air_pollution.scripts.merge_air_pollution import (
    merge_air_pollution_data,
)
from dags.weather_and_air_pollution.scripts.merge_weather import merge_weather_data

# Configuration par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 27),
}

# Liste des villes à traiter
cities = ["Seoul", "San Fransisco", "Antananarivo"]

with DAG(
    "weather_and_air_pollution_etl",
    default_args=default_args,
    schedule="@daily",  # Exécution quotidienne
    catchup=False,  # Ne pas rattraper les exécutions passées
    max_active_runs=1,  # Pour éviter les conflits
) as dag:
    # ========== Tâches d'Extraction ==========
    extract_weather_data_tasks = [
        PythonOperator(
            task_id=f"extract_weather_data_{city.lower().replace(' ', '_')}",
            python_callable=get_current_weather_data,
            op_args=[city, "{{ var.value.API_KEY }}"],
            # ds = date de l'exécution au format YYYY-MM-DD
        )
        for city in cities
    ]
    extract_air_pollution_data_tasks = [
        PythonOperator(
            task_id=f"extract_air_pollution_data_{city.lower().replace(' ', '_')}",
            python_callable=get_current_air_pollution_data,
            op_args=[city, "{{ var.value.API_KEY }}"],
            # ds = date de l'exécution au format YYYY-MM-DD
        )
        for city in cities
    ]
    # ========== Tâche de Fusion ==========
    merge_weather_data_task = PythonOperator(
        task_id="merge_weather_data",
        python_callable=merge_weather_data,
        op_args=[cities],
    )
    merge_air_pollution_data_task = PythonOperator(
        task_id="merge_air_pollution",
        python_callable=merge_air_pollution_data,
        op_args=[cities],
    )

    # ========== Orchestration ==========
    # Les tâches d'extraction s'exécutent en parallèle
    # puis la fusion s'exécute, suivie de la transformation
    extract_weather_data_tasks >> merge_weather_data_task
    extract_air_pollution_data_tasks >> merge_air_pollution_data_task
