''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from eia_natural_gas_spot_prices_etl_pipeline_dag.extraction import natural_gas_spot_prices_extraction
from eia_natural_gas_spot_prices_etl_pipeline_dag.drop_columns import drop_columns
from eia_natural_gas_spot_prices_etl_pipeline_dag.drop_nulls import drop_nulls
from eia_natural_gas_spot_prices_etl_pipeline_dag.rename_columns import rename_columns
from eia_natural_gas_spot_prices_etl_pipeline_dag.convert_values_to_float import convert_values_to_float
from eia_natural_gas_spot_prices_etl_pipeline_dag.extend_previous_data import extend_previous_data
from eia_natural_gas_spot_prices_etl_pipeline_dag.data_quality_checks import data_quality_checks

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs weekly
with DAG(dag_id='natural_gas_spot_prices_etl_pipeline', default_args=default_args, schedule_interval = timedelta(days=7, hours=2), 
        catchup=False) as dag:
    natural_gas_spot_prices_extraction_task = PythonOperator(
        task_id='natural_gas_spot_prices_extraction',
        python_callable=natural_gas_spot_prices_extraction
    )
    drop_columns_task = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns
    )
    drop_nulls_task = PythonOperator(
        task_id='drop_nulls',
        python_callable=drop_nulls
    )
    convert_values_to_float_task = PythonOperator(
        task_id='convert_values_to_float',
        python_callable=convert_values_to_float
    )
    rename_columns_task = PythonOperator(
        task_id='rename_columns',
        python_callable=rename_columns
    )
    extend_previous_data_task = PythonOperator(
        task_id='extend_previous_data',
        python_callable = extend_previous_data
    )
    data_quality_checks_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable = data_quality_checks
    )

    natural_gas_spot_prices_extraction_task >> drop_columns_task >> drop_nulls_task >> convert_values_to_float_task >> rename_columns_task \
    >> extend_previous_data_task >> data_quality_checks_task