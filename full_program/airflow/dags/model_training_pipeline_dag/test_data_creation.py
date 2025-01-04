''' Import modules '''
from datetime import datetime, timedelta
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def test_data_creation():
    ''' Function that creates test data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3 folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Create test data
    curated_test_data_df = EtlTransforms.create_test_data(df=curated_training_data_df, holdout=0.2)

    # Reset index so date column is stored as json
    curated_test_data_df = curated_test_data_df.reset_index()

    # Convert date from timestamp to string
    curated_test_data_df['date'] = curated_test_data_df['date'].dt.strftime('%Y-%m-%d')

    # Put test data in S3
    s3.put_data(data=curated_test_data_df, folder='full_program/curated/test_data/', object_key=f'curated_test_data_{formatted_date}')