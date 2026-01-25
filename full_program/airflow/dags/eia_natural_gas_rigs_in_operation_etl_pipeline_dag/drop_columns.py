''' Import modules '''
from datetime import datetime
import pandera as pa
from pandera import Column, Check
import pandas as pd
from dags.utils.config import *
from dags.extraction.eia_api import *
from dags.transformation.etl_transforms import EtlTransforms
from dags.utils.data_quality_check_functions import DataQualityChecks

def drop_columns():
    ''' Drop irrelevant columns from extracted natural gas rigs in operation '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    natural_gas_rigs_in_operation_json = s3.get_data(folder='full_program/extraction/natural_gas_rigs_in_operation/', object_key=f'natural_gas_rigs_in_operation_{formatted_date}')
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
    }
    )
    schema.validate(natural_gas_rigs_in_operation_df)

    # Drop null values from natural_gas_rigs_in_operation_df
    natural_gas_rigs_in_operation_df = EtlTransforms.drop_columns(df=natural_gas_rigs_in_operation_df, columns=['duoarea', 'area-name', 'product', 'product-name', 'process',
    'series', 'series-description', 'units'])
    
    # Put data in S3
    s3.put_data(data=natural_gas_rigs_in_operation_df, folder='full_program/transformation/natural_gas_rigs_in_operation/', object_key=f'natural_gas_rigs_in_operation_{formatted_date}')