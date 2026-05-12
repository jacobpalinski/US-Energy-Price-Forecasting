# Import modules
from datetime import datetime, timedelta
import pandas as pd
import pandera as pa
from pandera import Column, Check
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def data_quality_checks():
    ''' Function that performs data quality checks on transformed natural gas rigs in operation dataset '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest and previous transformed data from S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('natural_gas_rigs_in_operation', {}).get('latest_transformed_file_path')
    natural_gas_rigs_in_operation_transformed_json = s3.get_data(s3_key=latest_transformed_file_path)
    natural_gas_rigs_in_operation_transformed_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_transformed_json, date_as_index=False)

    # Log row count of latest transformed dataset
    logger.info(f"Latest transformed dataset contains {len(natural_gas_rigs_in_operation_transformed_df)} rows")
    
    previous_transformed_file_path = metadata.get('natural_gas_rigs_in_operation', {}).get('previous_transformed_file_path')
    if previous_transformed_file_path is not None:
        natural_gas_rigs_in_operation_previous_transformed_json = s3.get_data(s3_key=previous_transformed_file_path)
        natural_gas_rigs_in_operation_previous_transformed_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_previous_transformed_json, date_as_index=False)
        # Log row count of previous transformed dataset
        logger.info(f"Previous transformed dataset contains {len(natural_gas_rigs_in_operation_previous_transformed_df)} rows")
    else:
        natural_gas_rigs_in_operation_previous_transformed_df = None

    # Retrieve start and end dates for data quality checks
    if natural_gas_rigs_in_operation_previous_transformed_df is not None:
        start_date = natural_gas_rigs_in_operation_previous_transformed_df['date'].iloc[0]
    else:
        start_date = natural_gas_rigs_in_operation_transformed_df['date'].iloc[0]

    end_date = natural_gas_rigs_in_operation_transformed_df['date'].iloc[-1]

    # Log start and end dates
    logger.info(f"Start date for data quality checks: {start_date}")
    logger.info(f"End date for data quality checks: {end_date}")

    # Pandera schema for data quality checks
    schema = pa.DataFrameSchema(
    columns={
        "date": Column(str, nullable=False, checks=[Check(lambda s: pd.to_datetime(s).ge(start_date).all(), element_wise=False),
        Check(lambda s: pd.to_datetime(s).le(end_date).all(), element_wise=False),]),
        "natural_gas_rigs_in_operation": Column(int, nullable=False, checks=Check.ge(0))
    },
    strict=True,
    unique=["date", "natural_gas_rigs_in_operation"])

    # Validate schema
    try:
        schema.validate(natural_gas_rigs_in_operation_transformed_df)
        logger.info("Data quality checks passed")
    except pa.errors.SchemaError:
        logger.exception("Data quality validation failed")