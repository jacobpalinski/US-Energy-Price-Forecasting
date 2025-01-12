''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from eia_natural_gas_spot_prices_etl_pipeline_dag.extraction import natural_gas_spot_prices_extraction
from eia_natural_gas_spot_prices_etl_pipeline_dag.drop_columns import drop_columns
from eia_natural_gas_spot_prices_etl_pipeline_dag.drop_nulls import drop_nulls
from eia_natural_gas_spot_prices_etl_pipeline_dag.rename_columns import rename_columns
from eia_natural_gas_spot_prices_etl_pipeline_dag.convert_values_to_float import convert_values_to_float

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs weekly
with DAG(dag_id='natural_gas_spot_prices_etl_pipeline', default_args=default_args, schedule_interval = timedelta(days=7, hours=2), 
        catchup=False) as dag:
    natural_gas_spot_prices_extraction = PythonOperator(
        task_id='natural_gas_spot_prices_extraction',
        python_callable=natural_gas_spot_prices_extraction
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
    rename_columns = PythonOperator(
        task_id='rename_columns',
        python_callable=rename_columns
    )
    natural_gas_spot_prices_extraction >> drop_columns >> drop_nulls >> convert_values_to_float >> rename_columns