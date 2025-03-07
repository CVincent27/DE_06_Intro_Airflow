# https://airflow.apache.org/docs/apache-airflow/1.10.6/tutorial.html
from airflow import DAG
from airflow.operators.empty import EmptyOperator #EmptyOperator utilisé pour marquer le début et la fin du dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vincent',
    'depends_on_past': False, # Défini si les tâches sont dépendantes entre elles
    'start_date': datetime(2025, 3, 6), # date du jour
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    "premier_dag_test",
    default_args=default_args,
    description="Premier dag de test",
    schedule_interval=timedelta(days=1),

)

# Tâche dans le dag
def print_hello():
    return 'Hello world !'

#1ère tâche qui ne fait rien mais qui indique le début du dag
start = EmptyOperator(
    task_id='start',
    dag=dag
)

hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=print_hello,
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

 # Définition des dépendances des tâches
start >> hello_world >> end