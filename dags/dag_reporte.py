from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def generar_reporte():
    conn = psycopg2.connect(
        host='postgres', database='airflow', user='airflow', password='airflow'
    )
    cursor = conn.cursor()
    cursor.execute("DELETE FROM reporte_stock")

    cursor.execute("""
        INSERT INTO reporte_stock (producto, almacen, cantidad_original, unidades_vendidas, stock_actual)
        SELECT
            i.producto,
            i.almacen,
            i.cantidad,
            COALESCE(SUM(v.unidades), 0) as unidades_vendidas,
            i.cantidad - COALESCE(SUM(v.unidades), 0) as stock_actual
        FROM inventario i
        LEFT JOIN ventas v ON i.producto = v.producto
        GROUP BY i.producto, i.almacen, i.cantidad
    """)

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

with DAG(
    dag_id='dag_reporte_stock',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description='Genera un reporte de stock del producto y almacen'
) as dag:

    generar_reporte_task = PythonOperator(
        task_id='generar_reporte',
        python_callable=generar_reporte
    )