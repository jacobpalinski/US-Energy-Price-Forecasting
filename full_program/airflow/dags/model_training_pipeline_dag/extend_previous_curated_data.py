''' Import modules '''
from datetime import datetime, timedelta
import pandas as pd
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def extend_previous_curated_data():
    ''' Function that concatenates current curated data with previous curated data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Previous curated data
    previous = datetime.now() - timedelta(days=7)
    formatted_previous_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3 folder
    current_curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(data=current_curated_training_data_json, date_as_index=True)

    # Retrieve previous training data from S3 folder
    previous_curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_previous_date}')
    previous_curated_training_data_df = EtlTransforms.json_to_df(data=previous_curated_training_data_json, date_as_index=True)

    # Concatenate dataframes ensuring index is ordered
    curated_training_data_df = pd.concat([previous_curated_training_data_df, curated_training_data_df], sort=False).sort_index()

    # Put data in S3
    s3.put_data(data=curated_training_data_df, folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')