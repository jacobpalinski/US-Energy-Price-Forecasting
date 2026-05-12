# Import modules
from datetime import datetime, timedelta
import pandas as pd
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def extend_previous_curated_data(ts_nodash):
    ''' Function that concatenates current curated data with previous curated data '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve current and previous curated filepaths from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    current_processing_filepath = metadata.get('curated_training_data', {}).get('processing_file_path')
    previous_filepath = metadata.get('curated_training_data', {}).get('previous_file_path')

    # Create dataframes from latest and previous transformed filepaths if they exist and concatenate if previous transformed filepath exists. If previous transformed filepath does not exist, retain latest transformed dataframe as curated_training_data_df
    current_curated_training_data_json = s3.get_data(s3_key=current_processing_filepath)
    current_curated_training_data_df = EtlTransforms.json_to_df(data=current_curated_training_data_json, date_as_index=True)

    # Log row count of current curated dataset
    logger.info(f"Current curated dataset contains {len(current_curated_training_data_df)} rows")

    if previous_filepath is not None:
        previous_curated_training_data_json = s3.get_data(s3_key=previous_filepath)
        previous_curated_training_data_df = EtlTransforms.json_to_df(data=previous_curated_training_data_json, date_as_index=True)

        # Log row count of previous curated dataset
        logger.info(f"Previous curated dataset contains {len(previous_curated_training_data_df)} rows")

        curated_training_data_df = pd.concat([previous_curated_training_data_df, current_curated_training_data_df], sort=False).sort_index()
    else:
        curated_training_data_df = current_curated_training_data_df
    
    # Log row count of final dataset after concatenation
    logger.info(f"Final dataset after concatenation contains {len(curated_training_data_df)} rows")

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Retrieve latest transformed file path from metadata
    latest_filepath = metadata.get('curated_training_data', {}).get('latest_file_path')
    
    # Retrieve latest end date from dataframe
    latest_end_date = curated_training_data_df['date'].iloc[-1]

    # Put data in S3 and update metadata
    s3.put_data(data=curated_training_data_df, s3_key=f'full_program/curated/training_data/curated_training_data_{ts_nodash}.json')
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='curated_training_data', latest_end_date=latest_end_date, previous_filepath=latest_filepath, latest_filepath=current_processing_filepath)

