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

def extend_previous_data():
    ''' Function that concatenates previous natural gas spot prices transformed dataset with current dataset '''
    # Instantiate classes for Config, S3 and S3Metadata
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve latest extracted timestamp, latest transformed timestamp and previous transformed timestamp
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_extracted_timestamp = metadata.get('natural_gas_spot_prices', {}).get('latest_extracted_timestamp')
    latest_transformed_timestamp = metadata.get('natural_gas_spot_prices', {}).get('latest_transformed_timestamp')
    previous_transformed_timestamp = metadata.get('natural_gas_spot_prices', {}).get('previous_transformed_timestamp')

    # Log retrieved timestamps
    logger.info(f"Latest extracted timestamp: {latest_extracted_timestamp}")
    logger.info(f"Latest transformed timestamp: {latest_transformed_timestamp}")
    logger.info(f"Previous transformed timestamp: {previous_transformed_timestamp}")

    # Retrive latest transformed data from S3 and convert to dataframe with date as index
    latest_transformed_file_path = metadata.get('natural_gas_spot_prices', {}).get('latest_transformed_file_path')
    natural_gas_spot_prices_transformed_json = s3.get_data(s3_key=latest_transformed_file_path)
    natural_gas_spot_prices_transformed_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_transformed_json, date_as_index=True)

    # Log row count of latest transformed dataset
    logger.info(f"Latest transformed dataset contains {len(natural_gas_spot_prices_transformed_df)} rows")

    # Check if latest extracted timestamp is later than the previous transformed timestamp, provided previous timestamp exists.
    if previous_transformed_timestamp is not None:
        if latest_extracted_timestamp > previous_transformed_timestamp:
            # If latest extracted timestamp is later than previous transformed timestamp concatenate previous transformed dataset with latest transformed dataset
            previous_transformed_file_path = metadata.get('natural_gas_spot_prices', {}).get('previous_transformed_file_path')
            natural_gas_spot_prices_previous_transformed_json = s3.get_data(s3_key=previous_transformed_file_path)
            natural_gas_spot_prices_previous_transformed_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_previous_transformed_json, date_as_index=True)

            # Log row count of previous transformed dataset
            logger.info(f"Previous transformed dataset contains {len(natural_gas_spot_prices_previous_transformed_df)} rows")

            natural_gas_spot_prices_df = pd.concat([natural_gas_spot_prices_previous_transformed_df, natural_gas_spot_prices_transformed_df])
        else:
            # If latest extracted timestamp is not later than previous transformed timestamp, retain latest transformed dataset as natural_gas_spot_prices_df
            natural_gas_spot_prices_df = natural_gas_spot_prices_transformed_df
    else:
        # If latest extracted timestamp is not later than previous transformed timestamp, retain latest transformed dataset as natural_gas_spot_prices_df
        natural_gas_spot_prices_df = natural_gas_spot_prices_transformed_df

    # Log row count of final dataset after concatenation
    logger.info(f"Final dataset after concatenation contains {len(natural_gas_spot_prices_df)} rows")

    # Reset index
    natural_gas_spot_prices_df = natural_gas_spot_prices_df.reset_index()

    # Convert date from timestamp to string
    natural_gas_spot_prices_df['date'] = natural_gas_spot_prices_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=natural_gas_spot_prices_df, s3_key=latest_transformed_file_path)

    # Update metadata with so that previous and latest transformed file path are the same
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='natural_gas_spot_prices', previous_transformed_file_path=latest_transformed_file_path,
                                previous_transformed_timestamp=latest_transformed_timestamp)

