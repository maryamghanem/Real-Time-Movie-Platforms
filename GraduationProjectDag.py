from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import time

# Define the wait function for the PythonOperator
def wait(mini):
    time.sleep(mini * 60)

default_args = {
    'owner': 'Mostafa',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ProjectMoviesProducers',
    default_args=default_args,
    description='A DAG to run multiple producer scripts in parallel',
    schedule_interval='@daily',
    catchup=False,
    tags=['GraduationProject'],
) as dag:

    # Start DummyOperator
    start = DummyOperator(
        task_id='start',
        retries=3
    )

    # BashOperator tasks for each producer script
    disney = BashOperator(
        task_id='run_disneyproducer',
        bash_command='python /home/mostafa/airflow_env/Producers/disneyproducer.py',
    )

    amazon = BashOperator(
        task_id='run_amazonproducer',
        bash_command='python /home/mostafa/airflow_env/Producers/Amazonproducer.py',
    )

    netflix = BashOperator(
        task_id='run_netflixproducer',
        bash_command='python /home/mostafa/airflow_env/Producers/netflixproducer.py',
    )

    hulu = BashOperator(
        task_id='run_huluproducer',
        bash_command='python /home/mostafa/airflow_env/Producers/huluproducer.py',
    )

    # spark = BashOperator(
    #     task_id='run_Spark',
    #     bash_command='python /home/mostafa/airflow_env/Producers/Spark.py',
    # )

    # End DummyOperator
    end = DummyOperator(
        task_id='end',
        retries=3
    )

    # Set task dependencies
    start >> [hulu, disney, amazon, netflix] >> end
