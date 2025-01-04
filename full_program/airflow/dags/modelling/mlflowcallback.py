''' Import modules '''
import mlflow
from tensorflow.keras.callbacks import Callback, EarlyStopping

class MLflowCallback(Callback):
    '''
    Custom callback class to log loss and validation loss for tensorflow models
    
    Methods
    -------
    on_epoch_end(self, epoch, logs): Logs loss and validation loss after each epoch
    of model training
    '''
    def on_epoch_end(self, epoch: int, logs=None) -> None:
        ''' 
        Logs loss and validation loss after each epoch
        of model training

        Args:
            epoch (int): Epoch number
            logs (dict): Contains metrics computed during training of each epoch
        '''
        if logs is not None:
            mlflow.log_metric("loss", logs.get("loss"), step=epoch)
            mlflow.log_metric("val_loss", logs.get("val_loss"), step=epoch)