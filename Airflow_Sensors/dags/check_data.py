from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator


def processing_data():
    print("records exist!")


with DAG (
     "check_data",
    description="this is a dag for sales pipeline",
    start_date=datetime(2024,12,6,10,00),
    schedule_interval="*/30 * * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=45)
    ,tags=["sales","daily"]
) as dag :
    
    check_records=SqlSensor(
        task_id="check_records",
        conn_id="postgres_conn",
        sql=''' select * from customers where customer_name='Rafef'; ''',
        poke_interval=10,
        timeout=30,
        mode="reschedule",
        soft_fail=True
    )

    processing_data=PythonOperator(
        task_id="processing_data",
        python_callable=processing_data,

    )

    check_records >> processing_data
    