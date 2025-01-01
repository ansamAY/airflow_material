from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable



with DAG (
     "myfirstdag",
    description="this is a dag for sales pipeline",
    start_date=datetime(2024,12,6,10,00),
    schedule_interval="*/30 * * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=45)
    ,tags=["sales","daily"]
) as dag :
    
    
    
    create_table=PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_conn",
        sql='/sql/create_customer.sql',
    )

    insert_values=PostgresOperator(
        task_id="insert_values",
        postgres_conn_id="postgres_conn",
        sql='/sql/insert.sql',
    )

    select_values=PostgresOperator(
        task_id="select_values",
        postgres_conn_id="postgres_conn",
        sql=''' select * from customers where birth_date between %(start_date)s and %(end_date)s''',
        parameters={"start_date" : "{{var.value.start_date}}" , "end_date" : "{{var.value.end_date}}" }

    )

    create_table>>insert_values>>select_values





