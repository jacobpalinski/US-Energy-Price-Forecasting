# Import modules
from datetime import datetime, timedelta
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def test_data_creation(ts_nodash):
    ''' Function that creates test data '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve curated training data from S3 folder
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_filepath = metadata.get('curated_training_data', {}).get('latest_file_path')
    latest_curated_training_data_json = s3.get_data(s3_key=latest_filepath)
    latest_curated_training_data_df = EtlTransforms.json_to_df(data=latest_curated_training_data_json, date_as_index=True)

    # Log the number of rows in curated training data
    logger.info(f"Latest curated training dataset contains {len(latest_curated_training_data_df)} rows")

    # Create test data
    curated_test_data_df = EtlTransforms.create_test_data(df=latest_curated_training_data_df, holdout=0.2)

    # Log the number of rows in the test dataset
    logger.info(f"Test dataset contains {len(curated_test_data_df)} rows")

    # Reset index so date column is stored as json
    curated_test_data_df = curated_test_data_df.reset_index()

    # Convert date from timestamp to string
    curated_test_data_df['date'] = curated_test_data_df['date'].dt.strftime('%Y-%m-%d')

    # Put test data in S3
    s3.put_data(s3_key=f'full_program/curated/test_data/curated_test_data_{ts_nodash}.json')
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='curated_test_data', latest_file_path=f'full_program/curated/test_data/curated_test_data_{ts_nodash}.json')