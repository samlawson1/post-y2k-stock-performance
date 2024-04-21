from airflow import DAG 
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator

with DAG('stock_elt',
         start_date=datetime(2024, 4, 30),
         #run dag every weekday at 6:30 PM
         schedule_interval='30 18 * * 1-5',
         catchup=False) as dag:
    
    task_1 = DockerOperator(
        task_id = 'verify_s_and_p_500',
        docker_url='unix://var/run/docker.sock',
        api_version='auto',
        auto_remove=True,
        image='s_and_p_500_verification:v1',
        container_name='s_and_p_verification_container'
    )

    task_2A = DockerOperator(
        task_id = 'get_daily_stock_prices',
        docker_url='unix://var/run/docker.sock',
        api_version='auto',
        auto_remove=True,
        image='wiki-prices:v2',
        container_name='wiki_prices_container'
    )

    task_2B = DockerOperator(
        task_id = 'get_zacks_fc_data',
        docker_url='unix://var/run/docker.sock',
        api_version='auto',
        auto_remove=True,
        image='zacks-fc:v2',
        container_name='zacks_fc_api_container'
    )

    task_3 = DockerOperator(
        task_id = 'execute_dbt_model',
        docker_url='unix://var/run/docker.sock',
        api_version='auto',
        auto_remove=True,
        image='dbt_model:v1',
        container_name='dbt_stock_model_container'
    )

task_1 >> [task_2A, task_2B] >> task_3