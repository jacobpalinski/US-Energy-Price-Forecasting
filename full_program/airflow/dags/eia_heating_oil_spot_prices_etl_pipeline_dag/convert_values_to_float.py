# Import modules
from datetime import datetime
from dags.utils.config import Config
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.eia_api_transformation import EiaTransformation
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def convert_values_to_float():
    ''' Convert values to float from extracted heating oil spot prices '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest transformed filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('heating_oil_spot_prices', {}).get('latest_transformed_file_path')

    # Retrieve transformed data from S3 folder
    heating_oil_spot_prices_json = s3.get_data(latest_transformed_file_path)
    heating_oil_spot_prices_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json, date_as_index=False)

    # Convert column to float
    heating_oil_spot_prices_df = EiaTransformation.convert_column_to_numeric(df=heating_oil_spot_prices_df, column='value')
    
    # Log info about successful conversion
    logger.info("Successfully converted 'value' column to float")

    # Put data in S3
    s3.put_data(data=heating_oil_spot_prices_df, s3_key=latest_transformed_file_path)