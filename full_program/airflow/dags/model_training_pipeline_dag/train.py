''' Import modules '''
import os
import argparse
from datetime import datetime
import boto3
import mlflow
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.modelling.model import Model
from dags.modelling.mlflow_model import MlflowModel
from dags.transformation.etl_transforms import EtlTransforms

def parse_args():
    ''' Parses arguments for SageMaker training '''
    # Create ArgumentParser object
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument("--forecast_horizon", type=int)
    parser.add_argument("--sequence_length", type=int, default=30)
    parser.add_argument("--batch_size", type=int, default=128)

    # Return parsed arguments
    return parser.parse_args()


def main():
    ''' Main function for training model and saving model artifact for SageMaker '''
    # Parse arguments
    args = parse_args()

    # Setup mlflow tracking uri and retrieve experiment_id
    mlflow_model = MlflowModel(experiment_name=os.environ.get("EXPERIMENT_NAME"), tracking_uri=os.environ.get("MLFLOW_TRACKING_URI"))
    mlflow_model.set_tracking_uri()
    experiment_id = mlflow_model.retrieve_experiment_id()

    # Formatted date
    formatted_date = datetime.now().strftime('%Y%m%d')

    # Create connection to S3
    s3 = boto3.client('s3')

    # Retrieve curated training and test data from folder
    curated_training_data_json =s3.get_object(Bucket=os.getenv("S3_BUCKET"), Key=f'full_program/curated/training_data/curated_training_data_{formatted_date}')
    curated_test_data_json = s3.get_object(Bucket=os.getenv("S3_BUCKET"), Key=f'full_program/curated/test_data/curated_test_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(curated_training_data_json, date_as_index=True)
    curated_test_data_df = EtlTransforms.json_to_df(curated_test_data_json, date_as_index=True)

    # Normalise the data
    X_train = curated_training_data_df.drop(columns='price ($/MMBTU)')
    y_train = curated_training_data_df['price ($/MMBTU)']
    X_test = curated_test_data_df.drop(columns='price ($/MMBTU)')
    y_test = curated_test_data_df['price ($/MMBTU)']

    X_train, X_test = EtlTransforms.normalise(train_df=X_train, test_df=X_test)

    # Create sequences for training and test data
    train_dataset = EtlTransforms.build_dataset(X_train, y_train, args.sequence_length, args.batch_size)
    validation_dataset = EtlTransforms.build_dataset(X_test, y_test, args.sequence_length, args.batch_size)

    # Train GRU model
    Model.train_model(
        dataset=train_dataset,
        validation_dataset=validation_dataset,
        time_steps=args.sequence_length,
        experiment_id=experiment_id,
        forecast_horizon=args.forecast_horizon
    )

    # Save model for SageMaker
    model_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
    os.makedirs(model_dir, exist_ok=True)

    Model.save_model(os.path.join(model_dir, f"GRU_{args.forecast_horizon}_day_horizon_{args.sequence_length}_time_steps_{formatted_date}"))


if __name__ == "__main__":
    main()