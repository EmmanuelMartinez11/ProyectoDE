from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

from scripts.proyectoDE import crearDataLake, crearDataWarehouse, cargarDataLake, cargarDataWarehouse

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


    tarea_crear_data_lake = PythonOperator(
        task_id='crear_data_lake',
        python_callable=crearDataLake,
        op_kwargs={"stringDeConexion": "{{ ti.xcom_pull(task_ids='Obtener_string_de_conexion') }}"},
    )


    tarea_crear_data_warehouse = PythonOperator(
        task_id='crear_data_warehouse',
        python_callable=crearDataWarehouse,
        op_kwargs={
            "stringDeConexion": "{{ ti.xcom_pull(task_ids='Obtener_string_de_conexion') }}"
            },
    )

    dummy_final_de_creacion_de_tablas = DummyOperator(
        task_id="tablas_creadas"
    )

    tarea_cargar_data_lake = PythonOperator(
        task_id='cargar_data_lake',
        python_callable=cargarDataLake,
        op_kwargs={
            "ids_cryptos": ids_cryptos,
            "vs_currency": vs_currency,
            "price_change_percentage": price_change_percentage,
            "stringDeConexion": "{{ ti.xcom_pull(task_ids='Obtener_string_de_conexion') }}"
            },
    )

    tarea_cargar_data_warehouse = PythonOperator(
        task_id='cargar_data_warehouse',
        python_callable=cargarDataWarehouse,
        op_kwargs={
            "stringDeConexion": "{{ ti.xcom_pull(task_ids='Obtener_string_de_conexion') }}"
            },
    )

    tarea_crear_data_lake >> dummy_final_de_creacion_de_tablas
    tarea_crear_data_warehouse >> dummy_final_de_creacion_de_tablas
    dummy_final_de_creacion_de_tablas >> tarea_cargar_data_lake >> tarea_cargar_data_warehouse
