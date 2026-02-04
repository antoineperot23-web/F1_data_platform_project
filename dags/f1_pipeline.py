from airflow import DAG
from airflow.operators.PythonOperator import PythonOperator
from airflow.operators.bashoperator import BashOperator
from datetime import datetime, timedelta

import sys
sys.path.append('/scripts')

default_args = {
    'owner': 'f1-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'f1_full_pipeline',
    default_args=default_args,
    description='F1 Pipeline: Ingest → dbt → Tests',
    schedule_interval='@daily', #TODO: It's a good idea to launch the pipeline after each race weekend. Use the sessions key of 2026 for different start date 
    catchup=False,
    tags=['f1', 'dbt', 'production']
)

ingest_drivers = PythonOperator()
