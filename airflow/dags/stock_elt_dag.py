from datetime import datetime
from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

@dag(start_date=datetime(2024, 5, 1, 18,), schedule_interval="@daily", catchup=False)

def daily_etl_dag():
    
    verify_s_and_p_500_task = DockerOperator(
        task_id = 'verify_S&P_500',
        image='s_and_p_500_verification:v1',
        docker_url='tcp://docker-proxy:2375',
        network_mode='bridge'
    )