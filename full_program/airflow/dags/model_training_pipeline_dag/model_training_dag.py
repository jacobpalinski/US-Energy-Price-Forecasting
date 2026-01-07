''' Import modules '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.external_task import ExternalTaskSensor
from model_training_pipeline_dag.natural_gas_feature_engineering import natural_gas_feature_engineering
from model_training_pipeline_dag.weather_variables_feature_engineering import weather_variables_feature_engineering
from model_training_pipeline_dag.backfill_missing_values import backfill_missing_values
from model_training_pipeline_dag.forwardfill_missing_values import forwardfill_missing_values
from model_training_pipeline_dag.extend_previous_curated_data import extend_previous_curated_data
from model_training_pipeline_dag.test_data_creation import test_data_creation
from model_training_pipeline_dag.train_model_7day import train_model_7day
from model_training_pipeline_dag.train_model_14day import train_model_14day
from model_training_pipeline_dag.train_model_30day import train_model_30day
from model_training_pipeline_dag.train_model_60day import train_model_60day
from model_training_pipeline_dag.generate_forecasts import generate_forecasts
from model_training_pipeline_dag.data_quality_checks import data_quality_checks

# Retrieve current date
today = datetime.now()
formatted_date = today.strftime('%Y%m%d')

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Create DAG that runs on a weekly basis
with DAG(dag_id='model_training_pipeline', default_args=default_args, schedule_interval = timedelta(days=7, hours=12), 
        catchup=False) as dag:
    check_natural_gas_spot_prices_etl_pipeline_complete = ExternalTaskSensor(
        task_id='check_natural_gas_spot_prices_etl_pipeline_complete',
        external_dag_id='natural_gas_spot_prices_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_heating_oil_spot_prices_etl_pipeline_complete = ExternalTaskSensor(
        task_id='check_heating_oil_spot_prices_etl_pipeline_complete',
        external_dag_id='heating_oil_spot_prices_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_natural_gas_rigs_in_operation_etl_pipeline_complete = ExternalTaskSensor(
        task_id='check_natural_gas_rigs_in_operation_etl_pipeline_complete',
        external_dag_id='natural_gas_rigs_in_operation_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_natural_gas_monthly_variables_etl_pipeline_complete = ExternalTaskSensor(
        task_id='check_natural_gas_monthly_variables_etl_pipeline_complete',
        external_dag_id='natural_gas_monthly_variables_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_noaa_etl_pipeline_complete = ExternalTaskSensor(
        task_id='check_noaa_etl_pipeline_complete',
        external_dag_id='noaa_etl_pipeline',
        external_task_id='data_quality_checks'
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
    data_quality_checks = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks
    )
    
    # Dag execution
    check_natural_gas_spot_prices_etl_pipeline_complete >> check_heating_oil_spot_prices_etl_pipeline_complete >> check_natural_gas_rigs_in_operation_etl_pipeline_complete \
    check_natural_gas_monthly_variables_etl_pipeline_complete >> check_noaa_etl_pipeline_complete >> natural_gas_feature_engineering  >> weather_variables_feature_engineering \
    >> backfill_missing_values >> forwardfill_missing_values >> extend_previous_curated_data >> test_data_creation \
    >> train_model_7day >> train_model_14day >> train_model_30day >> train_model_60day >> generate_forecasts >> data_quality_checks
