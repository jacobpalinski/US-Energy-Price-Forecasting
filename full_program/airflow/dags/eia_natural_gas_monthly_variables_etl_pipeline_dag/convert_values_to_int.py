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

def convert_values_to_int():
    ''' Converts columns to integer values for extracted natural gas monthly variables '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('natural_gas_monthly_variables', {}).get('latest_transformed_file_path')

    # Retrieve extracted data from S3 folder
    natural_gas_monthly_variables_json = s3.get_data(s3_key=latest_transformed_file_path)
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=False)

    # Convert column values to integer
    natural_gas_monthly_variables_df = EiaTransformation.convert_column_to_numeric(df=natural_gas_monthly_variables_df, column='value')

    # Log info about successful conversion
    logger.info("Successfully converted 'value' column to integer")
    
    # Put data in S3
    s3.put_data(data=natural_gas_monthly_variables_df, s3_key=latest_transformed_file_path)