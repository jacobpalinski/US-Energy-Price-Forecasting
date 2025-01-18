''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from eia_natural_gas_monthly_variables_etl_pipeline_dag.extraction import natural_gas_monthly_variables_extraction
from eia_natural_gas_monthly_variables_etl_pipeline_dag.drop_columns import drop_columns
from eia_natural_gas_monthly_variables_etl_pipeline_dag.drop_nulls import drop_nulls
from eia_natural_gas_monthly_variables_etl_pipeline_dag.pivot_data import pivot_data
from eia_natural_gas_monthly_variables_etl_pipeline_dag.rename_columns import rename_columns
from eia_natural_gas_monthly_variables_etl_pipeline_dag.convert_values_to_float import convert_values_to_float
from eia_natural_gas_monthly_variables_etl_pipeline_dag.convert_date_format import convert_date_format
from eia_natural_gas_monthly_variables_etl_pipeline_dag.extend_previous_data import extend_previous_data

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 23),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs weekly
with DAG(dag_id='natural_gas_monthly_variables_etl_pipeline', default_args=default_args, schedule_interval = timedelta(days=7, hours=2), 
        catchup=False) as dag:
    natural_gas_monthly_variables_extraction = PythonOperator(
        task_id='natural_gas_monthly_variables_extraction',
        python_callable=natural_gas_monthly_variables_extraction
    )
    drop_columns = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns
    )
    drop_nulls = PythonOperator(
        task_id='drop_nulls',
        python_callable=drop_nulls
    )
    convert_values_to_float = PythonOperator(
        task_id='convert_values_to_float',
        python_callable=convert_values_to_float
    )
    pivot_data = PythonOperator(
        task_id='pivot_data',
        python_callable=pivot_data
    )
    rename_columns = PythonOperator(
        task_id='rename_columns',
        python_callable=rename_columns
    )
    convert_date_format = PythonOperator(
        task_id='convert_date_format',
        python_callable=convert_date_format
    )
    extend_previous_data = PythonOperator(
        task_id='extend_previous_data',
        python_callable=extend_previous_data
    )
    natural_gas_monthly_variables_extraction >> drop_columns >> drop_nulls >> convert_values_to_float \
    >> pivot_data >> rename_columns >> convert_date_format >> extend_previous_data