''' Import modules '''
from datetime import datetime
import pandera as pa
from pandera import Column, Check
import pandas as pd
from dags.extraction.eia_api import *
from dags.transformation.etl_transforms import EtlTransforms
from dags.utils.data_quality_check_functions import DataQualityChecks

def drop_columns(**context):
    ''' Drop irrelevant columns from extracted heating oil spot prices '''
    ts_nodash = context["ts_nodash"]

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve latest extracted filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_extracted_file_path = metadata.get('heating_oil_spot_prices', {}).get('latest_extracted_file_path')

    # Retrieve extracted data from S3 folder
    heating_oil_spot_prices_json = s3.get_data(s3_key=latest_extracted_file_path)
    heating_oil_spot_prices_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json, date_as_index=False)

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
        )
    },
    unique=["value", "period"])
    schema.validate(heating_oil_spot_prices_df)

    # Drop irrelevant columns from heating_oil_spot_prices_df
    heating_oil_spot_prices_df = EtlTransforms.drop_columns(df=heating_oil_spot_prices_df, columns=['duoarea', 'area-name', 'product', 'product-name', 'process',
    'process-name', 'series', 'series-description', 'units'])

    # Retain filename for extracted data to be used as filename for transformed data in S3
    latest_extracted_filename = latest_extracted_file_path.split('/')[-1]
    
    # Put data in S3 and update metadata with latest transformed file path
    s3.put_data(data=heating_oil_spot_prices_df, s3_key=f'full_program/transformation/heating_oil_spot_prices/heating_oil_spot_prices_{ts_nodash}.json')
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='heating_oil_spot_prices', latest_transformed_file_path=f'full_program/transformation/heating_oil_spot_prices/heating_oil_spot_prices_{ts_nodash}.json', latest_transformed_timestamp=ts_nodash)
