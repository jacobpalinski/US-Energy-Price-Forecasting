''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from model_training_pipeline_dag.merge_datasets import merge_dataframes
from model_training_pipeline_dag.natural_gas_feature_engineering import natural_gas_feature_engineering
from model_training_pipeline_dag.weather_variables_feature_engineering import weather_variables_feature_engineering
from model_training_pipeline_dag.backfill_missing_values import backfill_missing_values
from model_training_pipeline_dag.forwardfill_missing_values import forwardfill_missing_values
from model_training_pipeline_dag.drop_columns import drop_columns
from model_training_pipeline_dag.extend_previous_curated_data import extend_previous_curated_data
from model_training_pipeline_dag.test_data_creation import test_data_creation
from model_training_pipeline_dag.train_model_7day import train_model_7day
from model_training_pipeline_dag.train_model_14day import train_model_14day
from model_training_pipeline_dag.train_model_30day import train_model_30day
from model_training_pipeline_dag.train_model_60day import train_model_60day
from model_training_pipeline_dag.generate_forecasts import generate_forecasts

# Retrieve current date
today = datetime.now()
formatted_date = today.strftime('%Y%m%d')

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs on a weekly basis
with DAG(dag_id='model_training_pipeline', default_args=default_args, schedule_interval = timedelta(7), 
        catchup=False) as dag:
    check_natural_gas_spot_prices_s3 = S3KeySensor(
        task_id='check_natural_gas_spot_prices_s3',
        bucket_name='us-energy-price-forecasting',
        bucket_key=f'full_program/transformation/natural_gas_spot_prices/natural_gas_spot_prices_{formatted_date}',
        aws_conn_id='aws_connection',
        poke_interval=30,
        timeout=120,
        mode='poke',
    )
    merge_dataframes = PythonOperator(
        task_id='merge_dataframes',
        python_callable=merge_dataframes
    )
    weather_variables_feature_engineering = PythonOperator(
        task_id='weather_variables_feature_engineering',
        python_callable=weather_variables_feature_engineering
    )
    natural_gas_feature_engineering = PythonOperator(
        task_id='natural_gas_feature_engineering',
        python_callable=natural_gas_feature_engineering
    )
    backfill_missing_values = PythonOperator(
        task_id='backfill_missing_values',
        python_callable=backfill_missing_values
    )
    forwardfill_missing_values = PythonOperator(
        task_id='forwardfill_missing_values',
        python_callable=forwardfill_missing_values
    )
    extend_previous_curated_data = PythonOperator(
        task_id='extend_previous_curated_data',
        python_callable=extend_previous_curated_data
    )
    test_data_creation = PythonOperator(
        task_id='test_data_creation',
        python_callable=test_data_creation
    )
    train_model_7day = PythonOperator(
        task_id='train_model_7day',
        python_callable=train_model_7day
    )
    train_model_14day = PythonOperator(
        task_id='train_model_14day',
        python_callable=train_model_14day
    )
    train_model_30day = PythonOperator(
        task_id='train_model_30day',
        python_callable=train_model_30day
    )
    train_model_60day = PythonOperator(
        task_id='train_model_60day',
        python_callable=train_model_60day
    )
    generate_forecasts = PythonOperator(
        task_id='generate_forecasts',
        python_callable=generate_forecasts
    )
    # Dag execution
    check_natural_gas_spot_prices_s3 >> merge_dataframes >> weather_variables_feature_engineering >> natural_gas_feature_engineering \
    >> backfill_missing_values >> forwardfill_missing_values >> extend_previous_curated_data >> test_data_creation \
    >> train_model_7day >> train_model_14day >> train_model_30day >> train_model_60day >> generate_forecasts
