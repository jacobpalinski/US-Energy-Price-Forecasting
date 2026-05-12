# Import modules
from datetime import datetime
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def forwardfill_missing_values(ts_nodash):
    ''' Function that forwardfills missing values as a result of feature engineering '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    current_processing_filepath = metadata.get('curated_training_data', {}).get('processing_file_path')
    curated_training_data_json = s3.get_data(s3_key=current_processing_filepath)
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Forwardfill missing values
    curated_training_data_df = EtlTransforms.forwardfill_null_values_end_of_series(df=curated_training_data_df, 
    columns=['imports', 'lng_imports', 'natural_gas_rigs_in_operation',
    'total_consumption_total_underground_storage_ratio', 'hdd_max', 'cdd_max', 'wci_sum', 'snow_sum', 
    'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median'
    ])

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Log forwardfill of missing values successful
    logger.info("Successfully forwardfilled missing values for curated training dataset")

    # Put data in S3
    s3.put_data(data=curated_training_data_df, s3_key=f'full_program/curated/training_data/curated_training_data_{ts_nodash}.json')

