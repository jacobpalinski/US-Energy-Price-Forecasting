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
from noaa_etl_pipeline_dag.data_quality_checks import data_quality_checks

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs weekly
with DAG(dag_id='noaa_etl_pipeline', default_args=default_args, schedule_interval = timedelta(days=7, hours=2), 
        catchup=False) as dag:
    noaa_extraction_task = PythonOperator(
        task_id='noaa_extraction',
        python_callable=noaa_extraction
    )
    modify_date_format_task = PythonOperator(
        task_id='modify_date_format',
        python_callable=modify_date_format
    )
    impute_missing_values_task = PythonOperator(
        task_id='impute_missing_values',
        python_callable=impute_missing_values
    )
    calculate_missing_average_temperature_task = PythonOperator(
        task_id='calculate_missing_average_temperature',
        python_callable=calculate_missing_average_temperature
    )
    pivot_data_task = PythonOperator(
        task_id='pivot_data',
        python_callable=pivot_data
    )
    drop_columns_task = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns
    )
    rename_columns_task = PythonOperator(
        task_id='rename_columns',
        python_callable=rename_columns
    )
    data_quality_checks_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks
    )
    noaa_extraction_task >> modify_date_format_task >> impute_missing_values_task >> calculate_missing_average_temperature_task \
    >> pivot_data_task >> drop_columns_task >> rename_columns_task >> data_quality_checks_task








