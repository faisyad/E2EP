import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd

#DAG
default_args = {
    'owner' : 'hamba Allah',
    'start_date' : datetime(2023, 5, 24),
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 2),
}

with DAG(
    dag_id = 'end-to-end23',
    description = 'coba terus sampe berhasil',
    default_args = default_args,
    schedule = '@daily'
) as dag:
    # #get data
    # ngambildata = PythonOperator(
    #     task_id = 'get_data',
    #     python_callable = semogabisa
    # )

    masukinkehadup = BashOperator(
        task_id = 'input_ke_hadoop',
        bash_command = 'hadoop fs -put -f /home/faisyadd/endtoend1/greentaxi2 /user/faisyadd/e2eproject'
    )

    bikintablestaging = BashOperator(
        task_id = 'table_hive',
        bash_command = 'python3 /mnt/c/Users/user/Documents/Airflow/dags/e3ep1.py'
    )

    dimptable = BashOperator(
        task_id = 'dim_transformasi',
        bash_command = 'python3 /mnt/c/Users/user/Documents/Airflow/dags/dimdiman.py'
    )
    
    facttable = BashOperator(
        task_id = 'fact_transformasi',
        bash_command = 'python3 /mnt/c/Users/user/Documents/Airflow/dags/fakta.py'
    )

    facttable >> dimptable >> bikintablestaging >> masukinkehadup 
    # >> ngambildata
    # staginghive= HiveOperator(
    #     task_id='hive_task1',
    #     hql=open('/mnt/c/Users/user/Documents/Airflow/dags/hive_querry.sql', 'r').read(),
    #     hive_cli_conn_id='hive_default'
    # )
    # transtv = PythonOperator
    # #copy to hadoop
    # storefiletohadoop = BashOperator(
    #     task_id = 'load_data_to_hadoop',
    #     bash_command = ('hadoop fs -copyFromLocal /home/faisyadd/endtoend1/csv /user/faisyadd/endtoend')
    # )
    #


