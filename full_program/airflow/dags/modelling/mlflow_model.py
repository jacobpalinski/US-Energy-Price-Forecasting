''' Import modules '''
import mlflow
from mlflow.tracking import MlflowClient

class MlflowModel:
    ''' 
    Class for retrieving ml flow experiment and experiment id to 
    be used for logging purposes when training machine learning model

    Instance Variables
    ------------------
    experiment_name (str): Name of experiment where models are going to be logged and registered
    tracking_uri (str): Uri for mlflow
    client (MlflowClient): Instatiated MlflowClient instance
    
    Methods
    -------
    set_tracking_uri(self):
        Sets mlflow tracking uri
    retrieve_experiment_id(self):
        Retrieves experiment id for a given experiment
    '''
    def __init__(self, experiment_name: str, tracking_uri: str) -> None:
        self.experiment_name = experiment_name
        self.tracking_uri = tracking_uri
        self.client = MlflowClient()
    
    def set_tracking_uri(self) -> None:
        ''' 
        Sets mlflow tracking uri 
        '''
        mlflow.set_tracking_uri(self.tracking_uri)
    
    def retrieve_experiment_id(self) -> str:
        ''' 
        Retrieves experiment id for a given experiment

        Returns:
            str: Experiment id in string format 
        '''
        experiment = mlflow.get_experiment_by_name(self.experiment_name)
        experiment_id = experiment.experiment_id
        return experiment_id