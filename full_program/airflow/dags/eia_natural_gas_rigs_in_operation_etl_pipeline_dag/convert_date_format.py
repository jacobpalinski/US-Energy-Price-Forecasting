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

def convert_date_format():
    ''' Convert date format for date column from extracted natural gas rigs in operation '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('natural_gas_rigs_in_operation', {}).get('latest_transformed_file_path')

    # Retrieve extracted data from S3 folder
    natural_gas_rigs_in_operation_json = s3.get_data(s3_key=latest_transformed_file_path)
    natural_gas_rigs_in_operation_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_json, date_as_index=False)

    # Convert date format for natural_gas_rigs_in_operation_df
    natural_gas_rigs_in_operation_df = EiaTransformation.convert_date_format(df=natural_gas_rigs_in_operation_df)

    # Log info about successful conversion of date format
    logger.info("Successfully converted date format for 'date' column in natural gas rigs in operation dataframe")
    
    # Put data in S3
    s3.put_data(data=natural_gas_rigs_in_operation_df, s3_key=latest_transformed_file_path)