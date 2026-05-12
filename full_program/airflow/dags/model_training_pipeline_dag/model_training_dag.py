# Import modules
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperator
from airflow.sensors.external_task import ExternalTaskSensor
from model_training_pipeline_dag.natural_gas_feature_engineering import natural_gas_feature_engineering
from model_training_pipeline_dag.weather_variables_feature_engineering import weather_variables_feature_engineering
from model_training_pipeline_dag.backfill_missing_values import backfill_missing_values
from model_training_pipeline_dag.forwardfill_missing_values import forwardfill_missing_values
from model_training_pipeline_dag.extend_previous_curated_data import extend_previous_curated_data
from model_training_pipeline_dag.test_data_creation import test_data_creation
from model_training_pipeline_dag.generate_forecasts import generate_forecasts
from model_training_pipeline_dag.data_quality_checks import data_quality_checks
from dags.utils.config import Config
from dags.utils.aws import SNSNotifier
from dotenv import load_dotenv
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Timestamp
ts_nodash = "{{ts_nodash}}"

# Import environment variables
load_dotenv()

# Setup config and SNS notifier classes
config = Config()
sns_notifier = SNSNotifier(config=config)

# BranchPythonOperator function
def decide_skip_or_run(**kwargs):
    ''' Function that determines whether or not tasks are skipped after natural_gas_feature_engineering '''
    ti = kwargs["ti"]
    should_skip = ti.xcom_pull(task_ids="natural_gas_feature_engineering")

    if should_skip:
        return "test_data_creation"
    return "weather_variables_feature_engineering"

# Create default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': sns_notifier
}

# Create DAG that runs on a weekly basis
with DAG(dag_id='model_training_pipeline', default_args=default_args, schedule_interval = timedelta(days=7, hours=12), 
        catchup=False) as dag:
    check_natural_gas_spot_prices_etl_pipeline_complete_task = ExternalTaskSensor(
        task_id='check_natural_gas_spot_prices_etl_pipeline_complete',
        external_dag_id='natural_gas_spot_prices_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_heating_oil_spot_prices_etl_pipeline_complete_task = ExternalTaskSensor(
        task_id='check_heating_oil_spot_prices_etl_pipeline_complete',
        external_dag_id='heating_oil_spot_prices_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_natural_gas_rigs_in_operation_etl_pipeline_complete_task = ExternalTaskSensor(
        task_id='check_natural_gas_rigs_in_operation_etl_pipeline_complete',
        external_dag_id='natural_gas_rigs_in_operation_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_natural_gas_monthly_variables_etl_pipeline_complete_task = ExternalTaskSensor(
        task_id='check_natural_gas_monthly_variables_etl_pipeline_complete',
        external_dag_id='natural_gas_monthly_variables_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    check_noaa_etl_pipeline_complete_task = ExternalTaskSensor(
        task_id='check_noaa_etl_pipeline_complete',
        external_dag_id='noaa_etl_pipeline',
        external_task_id='data_quality_checks'
    )
    natural_gas_feature_engineering_task = PythonOperator(
        task_id='natural_gas_feature_engineering',
        python_callable=natural_gas_feature_engineering,
        op_kwargs={'ts_nodash': ts_nodash}
    )
    branch_task = BranchPythonOperator(
        task_id='branch_after_natural_gas_feature_engineering',
        python_callable=decide_skip_or_run
    )
    weather_variables_feature_engineering_task = PythonOperator(
        task_id='weather_variables_feature_engineering',
        python_callable=weather_variables_feature_engineering,
        op_kwargs={'ts_nodash': ts_nodash}
    )
    backfill_missing_values_task = PythonOperator(
        task_id='backfill_missing_values',
        python_callable=backfill_missing_values,
        op_kwargs={'ts_nodash': ts_nodash}
    )
    forwardfill_missing_values_task = PythonOperator(
        task_id='forwardfill_missing_values',
        python_callable=forwardfill_missing_values,
        op_kwargs={'ts_nodash': ts_nodash}
    )
    extend_previous_curated_data_task = PythonOperator(
        task_id='extend_previous_curated_data',
        python_callable=extend_previous_curated_data,
        op_kwargs={'ts_nodash': ts_nodash}
    )
    data_quality_checks_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks
    )
    test_data_creation_task = PythonOperator(
        task_id='test_data_creation',
        python_callable=test_data_creation,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        op_kwargs={'ts_nodash': ts_nodash}
    )
    train_model_7day_task = SageMakerTrainingOperator(
    task_id="train_model_7day",
    config={
        "TrainingJobName": f"GRU-7-day-horizon-30-time_steps-{ts_nodash}",
        "RoleArn": "arn:aws:iam::320264762283:role/US-Energy-Price-Forecasting-Sagemaker-Execution-Role",

        "AlgorithmSpecification": {
            "TrainingImage": "320264762283.dkr.ecr.ap-southeast-1.amazonaws.com/us-energy-price-forecasting/model-training",
            "TrainingInputMode": "File"
        },

        "ResourceConfig": {
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 30
        },

        "StoppingCondition": {
            "MaxRuntimeInSeconds": 1800
        },

        "HyperParameters": {
            "forecast_horizon": "7",
            "sequence_length": "30",
            "batch_size": "128",
            "ts_nodash": ts_nodash
        },

        "Environment": {
            "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI"),
            "EXPERIMENT_NAME": os.environ.get("EXPERIMENT_NAME"),
            "S3_BUCKET": os.environ.get("S3_BUCKET")
        },

        "OutputDataConfig": {
            "S3OutputPath": "s3://us-energy-price-forecasting/full_program/artifacts/training/sagemaker/"
        }
    },
    wait_for_completion=True,
    check_interval=30,
    aws_conn_id='aws_connection'
    )
    train_model_14day_task = SageMakerTrainingOperator(
    task_id="train_model_14day",
    config={
        "TrainingJobName": f"GRU-14-day-horizon-21-time-steps_{ts_nodash}",
        "RoleArn": "arn:aws:iam::320264762283:role/US-Energy-Price-Forecasting-Sagemaker-Execution-Role",

        "AlgorithmSpecification": {
            "TrainingImage": "320264762283.dkr.ecr.ap-southeast-1.amazonaws.com/us-energy-price-forecasting/model-training",
            "TrainingInputMode": "File"
        },

        "ResourceConfig": {
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 30
        },

        "StoppingCondition": {
            "MaxRuntimeInSeconds": 1800
        },

        "HyperParameters": {
            "forecast_horizon": "14",
            "sequence_length": "21",
            "batch_size": "128",
            "ts_nodash": ts_nodash
        },

        "Environment": {
            "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI"),
            "EXPERIMENT_NAME": os.environ.get("EXPERIMENT_NAME"),
            "S3_BUCKET": os.environ.get("S3_BUCKET")
        },

        "OutputDataConfig": {
            "S3OutputPath": "s3://us-energy-price-forecasting/full_program/artifacts/training/sagemaker/"
        }
    },
    wait_for_completion=True,
    check_interval=30,
    aws_conn_id='aws_connection'
    )
    train_model_30day_task = SageMakerTrainingOperator(
    task_id="train_model_30day",
    config={
        "TrainingJobName": f"GRU-30-day-horizon-14-time-steps-{ts_nodash}",
        "RoleArn": "arn:aws:iam::320264762283:role/US-Energy-Price-Forecasting-Sagemaker-Execution-Role",

        "AlgorithmSpecification": {
            "TrainingImage": "320264762283.dkr.ecr.ap-southeast-1.amazonaws.com/us-energy-price-forecasting/model-training",
            "TrainingInputMode": "File"
        },

        "ResourceConfig": {
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 30
        },

        "StoppingCondition": {
            "MaxRuntimeInSeconds": 1800
        },

        "HyperParameters": {
            "forecast_horizon": "30",
            "sequence_length": "14",
            "batch_size": "128",
            "ts_nodash": ts_nodash
        },

        "Environment": {
            "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI"),
            "EXPERIMENT_NAME": os.environ.get("EXPERIMENT_NAME"),
            "S3_BUCKET": os.environ.get("S3_BUCKET")
        },

        "OutputDataConfig": {
            "S3OutputPath": "s3://us-energy-price-forecasting/full_program/artifacts/training/sagemaker/"
        }
    },
    wait_for_completion=True,
    check_interval=30,
    aws_conn_id='aws_connection'
    )
    train_model_60day_task = SageMakerTrainingOperator(
    task_id="train_model_60day",
    config={
        "TrainingJobName": f"GRU-60_day-horizon-14-time_steps-{ts_nodash}",
        "RoleArn": "arn:aws:iam::320264762283:role/US-Energy-Price-Forecasting-Sagemaker-Execution-Role",

        "AlgorithmSpecification": {
            "TrainingImage": "320264762283.dkr.ecr.ap-southeast-1.amazonaws.com/us-energy-price-forecasting/model-training",
            "TrainingInputMode": "File"
        },

        "ResourceConfig": {
            "InstanceType": "ml.m5.xlarge",
            "InstanceCount": 1,
            "VolumeSizeInGB": 30
        },

        "StoppingCondition": {
            "MaxRuntimeInSeconds": 1800
        },

        "HyperParameters": {
            "forecast_horizon": "60",
            "sequence_length": "14",
            "batch_size": "128",
            "ts_nodash": ts_nodash
        },

        "Environment": {
            "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI"),
            "EXPERIMENT_NAME": os.environ.get("EXPERIMENT_NAME"),
            "S3_BUCKET": os.environ.get("S3_BUCKET")
        },

        "OutputDataConfig": {
            "S3OutputPath": "s3://us-energy-price-forecasting/full_program/artifacts/training/sagemaker/"
        }
    },
    wait_for_completion=True,
    check_interval=30,
    aws_conn_id='aws_connection'
    )
    generate_forecasts_task = PythonOperator(
        task_id='generate_forecasts',
        python_callable=generate_forecasts
    )
    
    # Dag execution
    check_natural_gas_spot_prices_etl_pipeline_complete_task \
    >> check_heating_oil_spot_prices_etl_pipeline_complete_task \
    >> check_natural_gas_rigs_in_operation_etl_pipeline_complete_task \
    >> check_natural_gas_monthly_variables_etl_pipeline_complete_task \
    >> check_noaa_etl_pipeline_complete_task \
    >> natural_gas_feature_engineering_task \
    >> branch_task

    # Branch 1: full processing path
    branch_task >> weather_variables_feature_engineering_task \
    >> backfill_missing_values_task \
    >> forwardfill_missing_values_task \
    >> extend_previous_curated_data_task \
    >> data_quality_checks_task \
    >> test_data_creation_task

    # Branch 2: Skip straight to test data creation if no new data in curated training dataset after feature engineering
    branch_task >> test_data_creation_task

    test_data_creation_task \
    >> train_model_7day_task \
    >> train_model_14day_task \
    >> train_model_30day_task \
    >> train_model_60day_task \
    >> generate_forecasts_task
