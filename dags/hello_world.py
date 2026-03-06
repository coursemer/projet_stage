from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG par défaut
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world',
    default_args=default_args,
    description='DAG de test Airflow',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def hello_world():
    print("Hello World from Airflow!")

def test_connectivity():
    print("Test de connectivité Airflow réussi!")

# Tâches
task1 = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag,
)

task2 = PythonOperator(
    task_id='test_connectivity',
    python_callable=test_connectivity,
    dag=dag,
)

task1 >> task2
