''' Import modules '''
from datetime import datetime
import pickle
import io
import os
import numpy as np
import pandas as pd
import mlflow
import tensorflow as tf
from tensorflow.python.keras.utils.version_utils import callbacks
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import Callback, EarlyStopping
from sklearn.metrics import mean_absolute_error
from dags.modelling.mlflowcallback import MLflowCallback
from dags.transformation.etl_transforms import EtlTransforms

class Model:
    ''' 
    Class used for modelling of data

    Methods
    -------
    train_model(cls, dataset, validation_dataset, time_steps, experiment_id, forecast_horizon):
        Trains model and logs model parameters, results and creates a model artifact to be used in streamlit 
    compile_model(cls, time_steps):
        GRU structure for model
    _train(cls, model, dataset, validation_dataset, epochs, callbacks):
        Trains model
    '''

    @classmethod
    def train_model(cls, dataset: tf.data.Dataset, validation_dataset: tf.data.Dataset, time_steps: int,
                    experiment_id: str, forecast_horizon: int,
                    ) -> None:
        '''
        Trains model and logs model parameters, results and creates a model artifact to be used in streamlit 

        Args:
            dataset (tf.data.Dataset): Training dataset
            validation_dataset (tf.data.Dataset): Validation dataset
            time_steps (int): Number of time steps used in forecast
            experiment_id (str): Mflow experiment id
            forecast_horizon: Length of time natural gas spot prices are being forecasted for
        '''
        # Create GRU model structure
        model = cls.compile_model(time_steps)

        # Retrieve current date
        current_date = datetime.now()
        current_date_formatted = current_date.strftime('%Y%m%d')

        # Define model name and versioning format for model registry
        model_name = f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}'

        with mlflow.start_run(experiment_id=experiment_id,
                              run_name=f'GRU_{forecast_horizon}_day_horizon_{time_steps}_{current_date_formatted}'):
            mlflow.log_param("units", 48)
            mlflow.log_param("activation_function", 'tanh')
            mlflow.log_param("dropout", 0.2)
            mlflow.log_param("epochs", 750)
            mlflow.log_param("batch_size", 128)

            # Train model
            trained = cls._train(model, dataset, validation_dataset, epochs=750, callbacks=[MLflowCallback()])

            # Log the model as an artifact
            model_artifact_path = model_name
            mlflow.keras.log_model(model, model_artifact_path)

            # Register the model in MLflow Model Registry
            artifact_uri = f"runs:/{mlflow.active_run().info.run_id}/{model_artifact_path}"
            mlflow.register_model(model_uri=artifact_uri, name=model_name)

    @classmethod
    def compile_model(cls, time_steps: int) -> keras.Sequential:
        '''
        GRU structure for model

        Args:
            time_steps (int): Number of time steps used in forecast
        
        Returns:
            keras.Sequential: GRU model structure
        '''
        model = keras.Sequential()
        model.add(layers.GRU(units=48, activation='tanh', return_sequences=True, input_shape=(time_steps, 27)))
        model.add(layers.GRU(units=48, activation='tanh'))
        model.add(layers.Dropout(0.2))
        model.add(layers.Dense(1))
        model.compile(optimizer='adam', loss='mae')
        return model

    @classmethod
    def _train(cls, model: keras.Sequential, dataset: tf.data.Dataset, validation_dataset: tf.data.Dataset, epochs: int, callbacks=None) -> keras.Sequential:
        '''
        Trains model

        Args:
            model (keras.Sequential): GRU model structure
            dataset (tf.data.Dataset): Training dataset
            validation_dataset (tf.data.Dataset): Validation dataset
            epochs (int): Number of epochs for model training
            callbacks: Custom callback class to log loss and validation loss for tensorflow model
        
        Returns:
            keras.Sequential: GRU model
        '''
        model.fit(dataset, epochs=epochs, validation_data=validation_dataset, verbose=2,
                  callbacks=[] if callbacks is None else callbacks)
        return model
