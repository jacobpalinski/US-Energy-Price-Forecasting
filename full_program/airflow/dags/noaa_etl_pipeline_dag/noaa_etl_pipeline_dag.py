''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from noaa_etl_pipeline_dag.extraction import noaa_extraction
from noaa_etl_pipeline_dag.modify_date_format import modify_date_format
from noaa_etl_pipeline_dag.impute_missing_values import impute_missing_values
from noaa_etl_pipeline_dag.calculate_missing_average_temperature import calculate_missing_average_temperature
from noaa_etl_pipeline_dag.pivot_data import pivot_data
from noaa_etl_pipeline_dag.drop_columns import drop_columns
from noaa_etl_pipeline_dag.rename_columns import rename_columns

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs weekly
with DAG(dag_id='noaa_etl_pipeline', default_args=default_args, schedule_interval = timedelta(7), 
        catchup=False) as dag:
    noaa_extraction = PythonOperator(
        task_id='noaa_extraction',
        python_callable=noaa_extraction
    )
    modify_date_format = PythonOperator(
        task_id='modify_date_format',
        python_callable=modify_date_format
    )
    impute_missing_values = PythonOperator(
        task_id='impute_missing_values',
        python_callable=impute_missing_values
    )
    calculate_missing_average_temperature = PythonOperator(
        task_id='calculate_missing_average_temperature',
        python_callable=calculate_missing_average_temperature
    )
    pivot_data = PythonOperator(
        task_id='pivot_data',
        python_callable=pivot_data
    )
    drop_columns = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns
    )
    rename_columns = PythonOperator(
        task_id='rename_columns',
        python_callable=rename_columns
    )
    noaa_extraction >> modify_date_format >> impute_missing_values >> calculate_missing_average_temperature \
    >> pivot_data >> drop_columns >> rename_columns








