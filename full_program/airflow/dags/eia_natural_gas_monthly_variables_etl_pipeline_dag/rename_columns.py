# Import modules
from datetime import datetime
from dags.utils.config import Config
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def rename_columns():
    ''' Rename columns from extracted natural gas monthly variables '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('natural_gas_monthly_variables', {}).get('latest_transformed_file_path')

    # Retrieve extracted data from S3 folder
    natural_gas_monthly_variables_json = s3.get_data(s3_key=latest_transformed_file_path)
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=False)

    # Rename pivoted columns
    natural_gas_monthly_variables_df = EtlTransforms.rename_columns(df=natural_gas_monthly_variables_df, renamed_columns={'Commercial Consumption': 'commercial_consumption', 
    'Imports': 'imports', 'Liquefied Natural Gas Imports': 'lng_imports', 'Residential Consumption': 'residential_consumption', 'Total Underground Storage': 'total_underground_storage',  
    'period': 'date'})

    # Log info about successful renaming of columns
    logger.info("Successfully renamed columns in natural gas monthly variables dataframe")
    
    # Put data in S3
    s3.put_data(data=natural_gas_monthly_variables_df, s3_key=latest_transformed_file_path)