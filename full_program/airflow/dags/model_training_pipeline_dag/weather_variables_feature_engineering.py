# Import modules
from datetime import datetime
import pandas as pd
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.noaa_api_transformation import NoaaTransformation
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def weather_variables_feature_engineering(ts_nodash):
    ''' Function that engineers features from weather variables and merges with curated natural gas data '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve daily_weather data from S3 folder
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_filepath = metadata.get('daily_weather', {}).get('latest_transformed_file_path')
    daily_weather_json = s3.get_data(s3_key=latest_transformed_filepath)
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=True)

    # Log row counts of daily weather dataset
    logger.info(f"Latest transformed daily weather dataset contains {len(daily_weather_df)} rows")

    # Retrieve current processing filepath for curated data
    current_processing_filepath = metadata.get('curated_training_data', {}).get('processing_file_path')
    curated_training_data_json = s3.get_data(s3_key=current_processing_filepath)
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Log row count of curated dataset
    logger.info(f"Latest curated dataset contains {len(curated_training_data_df)} rows")

    chunk_size = 10000
    processed_chunks = []

    # Create features from weather variables in daily_weather data
    for start in range(0, len(daily_weather_df), chunk_size):
        chunk = daily_weather_df.iloc[start: start + chunk_size]
        chunk = NoaaTransformation.maximum_hdd(df=chunk)
        chunk = NoaaTransformation.maximum_cdd(df=chunk)
        chunk = NoaaTransformation.wci_sum(df=chunk)
        chunk = NoaaTransformation.snow_sum(df=chunk)
        chunk = NoaaTransformation.min_and_max_average_temperature(df=chunk)
        chunk = NoaaTransformation.max_abs_tavg_diff(df=chunk)
        chunk = NoaaTransformation.max_abs_tavg_diff_relative_to_daily_median(df=chunk)
        processed_chunks.append(chunk)

    # Combine processed chunks
    daily_weather_df = pd.concat(processed_chunks)

    # Drop irrelevant columns
    daily_weather_df = EtlTransforms.drop_columns(df=daily_weather_df, columns=['city', 'state', 'quarter', 'tmin', 'tmax', 'tavg', 'snow', 'awnd'])

    # Log columns after drop_columns transformation
    logger.info(f"Daily weather dataset columns after drop_columns transformation: {daily_weather_df.columns.tolist()}")

    # Drop duplicated records
    daily_weather_df = daily_weather_df.drop_duplicates()

    # Log row counts of daily weather dataset after removing duplicates
    logger.info(f"Transformed daily weather dataset without duplicates contains {len(daily_weather_df)} rows")

    # Merge daily weather dataframe with curated training data
    curated_training_data_df = pd.merge(curated_training_data_df, daily_weather_df, left_index=True, right_index=True, how='left')

    # Log row count of curated dataset
    logger.info(f"Merged curated and daily weather dataset contains {len(curated_training_data_df)} rows")

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Log successful feature engineering
    logger.info('Features have been successfully engineered for curated training dataset')

    # Put data in S3
    s3.put_data(data=curated_training_data_df, s3_key=f'full_program/curated/training_data/curated_training_data_{ts_nodash}.json')