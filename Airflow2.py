from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import pandas as pd

def conexion_redshift(ds, execution_hour):
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base="data-engineer-database"
    user="fredafranco13_coderhouse"
    pwd= "uuS4769kWq"
    conn = psycopg2.connect(
        host=url,
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print(f"Conexión establecida a Redshift a las {execution_hour} del {ds}")
    return conn


def cargar_data(ds, execution_hour, conn):
    ruta_archivos='/app/'
    product = pd.read_excel(ruta_archivos+'Datos bancos.xlsx')
    cargar_en_redshift(conn, 'Datos_entrega2', product)
    print(f"Datos cargados a Redshift a las {execution_hour} del {ds}")


def cargar_en_redshift(conn, table_name, dataframe):
    cursor = conn.cursor()
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    cursor.execute(table_schema)
    for i, row in dataframe.iterrows():
        insert_query = f"INSERT INTO {table_name} VALUES {tuple(row.values)}"
        cursor.execute(insert_query)
    conn.commit()
    cursor.close()


with DAG('my_dag', start_date=datetime(2023, 6, 16), schedule_interval='@weekly') as dag:
    task_31 = PythonOperator(
        task_id="conexion_BD",
        python_callable=conexion_redshift,
        op_args=["{{ ds }}", "{{ execution_date.hour }}"],
    )

    task_32 = PythonOperator(
        task_id='cargar_data',
        python_callable=cargar_data,
        op_args=["{{ ds }}", "{{ execution_date.hour }}", task_31.output],
    )

    task_31 >> task_32

