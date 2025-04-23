from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

def load():
    conn = psycopg2.connect(
        host='postgres',
        dbname='airflow',
        user='airflow',
        password='airflow',
        port='5432'
    )
    cursor = conn.cursor()
    df = pd.read_csv('/opt/airflow/data/inventario.csv')

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO inventario (producto, cantidad, almacen) VALUES (%s, %s, %s)",
            (row['producto'], row['cantidad'], row['almacen'])
        )
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='dag_inventario',
    start_date=datetime(2025, 4, 6),
    schedule_interval=None,
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='cargar_datos_inventario',
        python_callable=load
    )

    load_task