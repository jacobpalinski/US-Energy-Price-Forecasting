''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from model_training_pipeline_dag.process_lambda_output import process_lambda_output
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

# Retrieve current date to be used by cloudwatch logs
today = datetime.now()
formatted_date = today.strftime('%Y%m%d')

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 10),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG
with DAG(dag_id='model_training_pipeline', default_args=default_args, schedule_interval = timedelta(30), 
        catchup=False) as dag:
    '''check_natural_gas_spot_prices_s3 = S3KeySensor(
        task_id='check_natural_gas_spot_prices_s3',
        bucket_name='us-energy-price-forecasting',  # Your S3 bucket name
        bucket_key=f'full_program/transformation/natural_gas_spot_prices/natural_gas_spot_prices_{formatted_date}',  # The S3 key (file path)
        aws_conn_id='aws_connection',  # The Airflow connection to AWS (ensure AWS credentials are set up)
        poke_interval=30,  # Time in seconds between each poll
        timeout=120,  # Timeout for the sensor
        mode='poke',  # The mode of operation ('poke' or 'reschedule')
    )'''
    '''merge_dataframes = PythonOperator(
        task_id='merge_dataframes',
        python_callable=merge_dataframes
    )
    natural_gas_feature_engineering = PythonOperator(
        task_id='natural_gas_feature_engineering',
        python_callable=natural_gas_feature_engineering
    )
    weather_variables_feature_engineering = PythonOperator(
        task_id='weather_variables_feature_engineering',
        python_callable=weather_variables_feature_engineering
    )
    backfill_missing_values = PythonOperator(
        task_id='backfill_missing_values',
        python_callable=backfill_missing_values
    )
    forwardfill_missing_values = PythonOperator(
        task_id='forwardfill_missing_values',
        python_callable=forwardfill_missing_values
    )
    drop_columns = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns
    )
    extend_previous_curated_data = PythonOperator(
        task_id='extend_previous_curated_data',
        python_callable=extend_previous_curated_data
    )
    test_data_creation = PythonOperator(
        task_id='test_data_creation',
        python_callable=test_data_creation
    )'''
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
    # Dag Execution
    train_model_7day >> train_model_14day >> train_model_30day >> train_model_60day
