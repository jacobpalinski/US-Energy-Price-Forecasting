# Import modules
from datetime import datetime
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def rename_columns():
    ''' Rename columns from extracted natural gas spot prices '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('natural_gas_spot_prices', {}).get('latest_transformed_file_path')

    # Retrieve extracted data from S3 folder
    natural_gas_spot_prices_json = s3.get_data(s3_key=latest_transformed_file_path)
    natural_gas_spot_prices_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_json, date_as_index=False)

    # Rename value and period columns from natural_gas_spot_prices_df
    natural_gas_spot_prices_df = EtlTransforms.rename_columns(df=natural_gas_spot_prices_df, renamed_columns={'value': 'price ($/MMBTU)', 'period': 'date'})

    # Log info about successful renaming of columns
    logger.info("Successfully renamed columns in natural gas spot prices dataframe")
    
    # Put data in S3
    s3.put_data(data=natural_gas_spot_prices_df, s3_key=latest_transformed_file_path)