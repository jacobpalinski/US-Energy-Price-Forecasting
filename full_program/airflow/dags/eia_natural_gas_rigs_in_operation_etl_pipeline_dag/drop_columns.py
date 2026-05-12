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
    ''' Drop irrelevant columns from extracted natural gas rigs in operation '''
    ts_nodash = context["ts_nodash"]

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_extracted_file_path = metadata.get('natural_gas_rigs_in_operation', {}).get('latest_extracted_file_path')

    # Retrieve extracted data from S3 folder
    natural_gas_rigs_in_operation_json = s3.get_data(s3_key=latest_extracted_file_path)
    natural_gas_rigs_in_operation_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_json, date_as_index=False)

    # Required process_name values
    required_process_names = {
        "Rotary Rigs in Operation"
    }

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
        "process-name": Column(
            object,
            checks=[
                Check(DataQualityChecks.check_is_string, element_wise=True),
                Check(
                    lambda s: required_process_names.issubset(set(s.dropna())),
                    element_wise=False,
                    error=f"process-name must include {required_process_names}",
                ),
            ],
            nullable=False,
        ),
    },
    unique=["value", "period", "process-name"]
    )
    
    try:
        schema.validate(natural_gas_rigs_in_operation_df)
        logger.info("Data quality checks passed")
    except pa.errors.SchemaError:
        logger.exception("Data quality validation failed")

    # Drop null values from natural_gas_rigs_in_operation_df
    natural_gas_rigs_in_operation_df = EtlTransforms.drop_columns(df=natural_gas_rigs_in_operation_df, columns=['duoarea', 'area-name', 'product', 'product-name', 'process',
    'series', 'series-description', 'units'])
    
    # Put data in S3
    s3.put_data(data=natural_gas_rigs_in_operation_df, s3_key=f'full_program/transformation/natural_gas_rigs_in_operation/natural_gas_rigs_in_operation_{ts_nodash}.json')
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='natural_gas_rigs_in_operation', latest_transformed_file_path=f'full_program/transformation/natural_gas_rigs_in_operation/natural_gas_rigs_in_operation_{ts_nodash}.json', latest_transformed_timestamp=ts_nodash)