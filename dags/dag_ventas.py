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
    df = pd.read_csv('/opt/airflow/data/ventas.csv')

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO ventas (producto, unidades, fecha) VALUES (%s, %s, %s)",
            (row['producto'], row['unidades'], row['fecha'])
        )
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='dag_ventas',
    start_date=datetime(2025, 4, 6),
    schedule_interval=None,
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='cargar_datos_ventas',
        python_callable=load
    )

    load_task