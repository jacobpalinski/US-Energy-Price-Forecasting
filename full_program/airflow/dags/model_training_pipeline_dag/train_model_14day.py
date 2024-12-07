''' Import modules '''
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
from dags.modelling.mlflow_model import MlflowModel
from dags.modelling.model import Model

def train_model_14day():
    ''' Function that trains model '''
    # Todays date
    #today = datetime.now()
    #formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Setup mlflow tracking uri and retrieve experiment_id
    mlflow_model = MlflowModel(experiment_name='Natural gas price forecasting production', tracking_uri='http://mlflow:5000')
    mlflow_model.set_tracking_uri()
    experiment_id = mlflow_model.retrieve_experiment_id()

    # Retrieve curated training and test data from folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key='curated_training_data_20241118')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=False) # Add date to index back
    curated_test_data_json = s3.get_data(folder='full_program/curated/test_data/', object_key='curated_test_data_20241118')
    curated_test_data_df = EtlTransforms.json_to_df(data=curated_test_data_json, date_as_index=False) # Add date to index back

    # Normalise the data
    X_train = curated_training_data_df.drop(columns='price ($/MMBTU)')
    y_train = curated_training_data_df['price ($/MMBTU)']
    X_test = curated_test_data_df.drop(columns='price ($/MMBTU)')
    y_test = curated_test_data_df['price ($/MMBTU)']

    X_train, X_test = EtlTransforms.normalise(train_df=X_train, test_df=X_test)

    # Create sequences for training and test data
    train_dataset_14day = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=21, batch_size=128)
    validation_dataset_14day = EtlTransforms.build_dataset(x=X_test, y=y_test, sequence_length=21, batch_size=128)
    print('sequences successfully created')

    # Train GRU model
    Model.train_model(train_dataset_14day, validation_dataset_14day, time_steps=21, experiment_id=experiment_id, forecast_horizon=14)
    print('models successfully trained')




    

