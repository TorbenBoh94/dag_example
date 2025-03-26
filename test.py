import random
from datetime import datetime
import time
import docker
from airflow import DAG
from airflow.decorators import task


client = docker.from_env()


with DAG(dag_id="dice", 
    schedule_interval=None,
    catchup=False,
) as dag:
    
    @task()
    def roll():
        return random.randint(0,6)

    @task()
    def is_even(num):
        if num % 2 == 0:
            return 1
        else:
            return 0
    @task()        
    def run_container(num):
        if num == 1:
            container = client.containers.get("b7462ab5ca97")
            container.start()

        else:
            container = client.containers.get("b7462ab5ca97")        
        time.sleep(30)
        container.stop()


    # Define task dependencies
    num = roll()
    even = is_even(num)
    container_run = run_container(even)

    num >> even #>> container_run