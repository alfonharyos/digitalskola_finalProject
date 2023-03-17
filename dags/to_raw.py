from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import psycopg2
import json
import os

default_args = {
    'owner' : 'alfonhs',
    'depend_on_past' : False,
    'start_date' : datetime(2023, 3, 16),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

#connect to potsgresql
conn = psycopg2.connect(
    host="localhost",
    database="final_project",
    user="postgres",
    password="12345678"
)

# open SQL file 
with open('create-raw-schema.sql', 'r') as f:
    sql_raw = f.read()
with open('create-ods-schema.sql', 'r') as f:
    sql_ods = f.read()
with open('create-serving-schema.sql', 'r') as f:
    sql_serving = f.read()

# run sql
cur  = conn.cursor()
cur.execute(sql_raw)
cur.execute(sql_ods)
cur.execute(sql_serving)
    
conn.commit()

# Menutup koneksi
cur.close()
conn.close()

def load_json_to_postgres(json_file_path, table_name, conn_id):
    with open(json_file_path) as f:
        data = json.load(f)
    columns = list(data[0].keys())
    values = [tuple(d.values()) for d in data]
    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s;"
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_hook.run(query, parameters=values)

with DAG('load_json_to_postgres', default_args=default_args, schedule_interval='@daily') as dag:
    
    load_data_task1 = PythonOperator(
        task_id='load_data_from_json1',
        python_callable=load_json_to_postgres,
        op_kwargs={
            'json_file_path': '/path/to/json/file1.json',
            'table_name': 'my_table',
            'conn_id': 'postgres_connection'
        }
    )

    load_data_task2 = PythonOperator(
        task_id='load_data_from_json2',
        python_callable=load_json_to_postgres,
        op_kwargs={
            'json_file_path': '/path/to/json/file2.json',
            'table_name': 'my_table',
            'conn_id': 'postgres_connection'
        }
    )

load_data_task1 >> load_data_task2
