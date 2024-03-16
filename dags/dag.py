from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator


ids_cryptos = ["bitcoin", "bitcoin-cash", "cardano", "dogecoin", "eos", "ethereum", "iota", "stellar", "litecoin", "neo"]
vs_currency = "usd"
price_change_percentage = "1h,24h,7d"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Definir DAG
with DAG(
    dag_id="crypto_data_pipeline",
    start_date=datetime(2024, 3, 12),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    # Tarea para crear la tabla de cryptos en la base de datos

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="proyectoDE_redshift",
        sql="sql/createsDataWarehouse.sql",
        params={"options": "-c search_path=emma_nionn_coderhouse_schema"}
    )
    # Definir dependencias
    dummy_start_task >> create_tables_task 
