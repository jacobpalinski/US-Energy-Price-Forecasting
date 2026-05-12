# Import modules
from datetime import datetime
import pandera as pa
from pandera import Column, Check
import pandas as pd
from dags.utils.config import Config
from dags.utils.aws import S3, S3Metadata
from dags.transformation.etl_transforms import EtlTransforms
from dags.utils.data_quality_check_functions import DataQualityChecks
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def drop_columns(**context):
    ''' Drop irrelevant columns from extracted natural gas spot prices '''
    # Timestamp of DAG execution
    ts_nodash = context["ts_nodash"]

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_extracted_file_path = metadata.get('natural_gas_spot_prices', {}).get('latest_extracted_file_path')

    # Retrieve extracted data from S3 folder
    natural_gas_spot_prices_json = s3.get_data(s3_key=latest_extracted_file_path)
    natural_gas_spot_prices_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_json, date_as_index=False)

    # Perform data quality checks on key columns 
    schema = pa.DataFrameSchema(
    {
        "value": Column(
            object,
            checks=Check(DataQualityChecks.is_numeric_or_null, element_wise=True),
            nullable=True,
        ),
        "period": Column(
            object,
            checks=[Check(DataQualityChecks.is_yyyy_mm_dd, element_wise=True),
            Check(lambda s: pd.to_datetime(s, errors="coerce").notna(), element_wise=False)],
            nullable=False,
        ),
    },
    unique=["value", "period"]
    )
    
    try:
        schema.validate(natural_gas_spot_prices_df)
        logger.info("Data quality checks passed")
    except pa.errors.SchemaError:
        logger.exception("Data quality validation failed")

    # Drop irrelevant columns from natural_gas_spot_prices_df
    natural_gas_spot_prices_df = EtlTransforms.drop_columns(df=natural_gas_spot_prices_df, columns=['duoarea', 'area-name', 'product', 'product-name', 'process',
    'process-name', 'series', 'series-description', 'units'])

    # Log columns after drop_columns transformation
    logger.info(f"Columns after drop_columns transformation: {natural_gas_spot_prices_df.columns.tolist()}")
    
    # Put data in S3 and update metadata with latest transformed file path
    s3.put_data(data=natural_gas_spot_prices_df, s3_key=f'full_program/transformation/natural_gas_spot_prices/natural_gas_spot_prices_{ts_nodash}.json')
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='natural_gas_spot_prices', latest_transformed_file_path=f'full_program/transformation/natural_gas_spot_prices/natural_gas_spot_prices_{ts_nodash}.json', latest_transformed_timestamp=ts_nodash)