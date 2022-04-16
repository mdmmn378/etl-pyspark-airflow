from airflow import DAG, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


with DAG(
    dag_id="dummy_dag",
    schedule_interval="@once",
    start_date=None,
    catchup=False,
    default_args={"owner": "airflow", "depends_on_past": False},
) as dag:
    task_dummy = PythonOperator(
        task_id="task_dummy", python_callable=lambda: print("Dummy task"), start_date=datetime.utcnow()+timedelta(seconds=10)
    )

# if __name__ == "__main__":
#     dag.cli()
