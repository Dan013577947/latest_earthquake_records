from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import sys

sys.path.append("/api_request")
def safe_callable_main():
    from insert_records import main
    return main()



default_args = {
    "description":"My Earthquake Dag",
    "start_date": datetime(2025,9,24),
    "catchup":False
}

dag = DAG(
    dag_id="Earthquake-Dag",
    default_args=default_args,
    schedule=timedelta(minutes=15)
)

with dag:
    task1 = PythonOperator(
        task_id="Raw-Earthquake-Data",
        python_callable=safe_callable_main
    )

    task2= DockerOperator(
        task_id="Earthquake-Dag-DBT",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command = 'run --select stg_earthquake_data hourly_earthquakes_count daily_earthquakes_count yearly_monthly_earthquakes_count daily_average_magnitude_depth most_active_locations 7d_rolling_count_and_avg_magnitude',
        working_dir= '/usr/app',
        mounts=[
            Mount(source="/home/reijin051824/repos/latest_earthquake_records/dbt/my_project",
            target='/usr/app',type='bind'),
            Mount(source="/home/reijin051824/repos/latest_earthquake_records/dbt/profiles.yml",
            target='/root/.dbt/profiles.yml',type='bind')
        ],
        network_mode="latest_earthquake_records_my-network",
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    task1 >> task2