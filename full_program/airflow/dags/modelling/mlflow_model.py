''' Import modules '''
import mlflow
from mlflow.tracking import MlflowClient

class MlflowModel:
    ''' Class for retrieving ml flow experiment and experiment id to 
    be used for logging purposes when training machine learning model '''
    def __init__(self, experiment_name: str, tracking_uri: str) -> None:
        self.experiment_name = experiment_name
        self.tracking_uri = tracking_uri
        self.client = MlflowClient()
    
    def set_tracking_uri(self) -> None:
        ''' Sets mlflow tracking uri '''
        mlflow.set_tracking_uri(self.tracking_uri)
    
    def retrieve_experiment_id(self) -> str:
        ''' Retrieves experiment id for a given experiment '''
        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        experiment_id = experiment.experiment_id
        return experiment_id

    def get_most_recent_model(self, model_name_prefix: str, experiment_id: str) -> None:
        ''' Retrieves most recent model for a specified prefix and experiment '''
        model = mlflow.pyfunc.load_model(model_uri="models:/GRU_7_day_horizon_30_20241226/1")
        if model:
            print('Successfully loaded model')
        filter_string = f"attributes.run_name LIKE '{model_name_prefix}'"
        print(f"Filter string: {filter_string}")
        runs = self.client.search_runs(
            experiment_id
            #filter_string=f"attributes.run_name LIKE '{model_name_prefix}'",
            #order_by = ['start_time desc'],
            #max_results=1
        )

        print(f"Runs found: {len(runs)}")

        if runs:
            latest_run = runs[0]
            print(f"Latest run: {latest_run.info.run_name}")
            model_name = latest_run.info.run_name
            run_id = latest_run.info.run_id
            model_uri = f"s3://us-energy-price-forecasting/full_program/artifacts/{run_id}/artifacts/{model_name}/data/model"
            print(f"Model URI: {model_uri}")
        else:
            raise ValueError(f"No runs found with model name prefix '{model_name_prefix}' in experiment {experiment_id}")
        
        model = mlflow.pyfunc.load_model(model_uri)
        return model