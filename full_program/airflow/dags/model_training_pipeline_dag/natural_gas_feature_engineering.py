''' Import modules '''
from datetime import datetime
import pandas as pd
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.transformation.eia_api_transformation import EiaTransformation
from dags.transformation.etl_transforms import EtlTransforms

def natural_gas_feature_engineering():
    ''' Function that engineers features from natural gas variables from natural gas datasets '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve natural gas spot prices, heating oil spot prices, natural gas monthly variables and natural gas rigs in operation data from S3 folder
    natural_gas_spot_prices_json = s3.get_data(folder='full_program/transformation/natural_gas_spot_prices/', object_key=f'natural_gas_spot_prices_{formatted_date}')
    heating_oil_spot_prices_json = s3.get_data(folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_date}')
    natural_gas_monthly_variables_json = s3.get_data(folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_rigs_in_operation_json = s3.get_data(folder='full_program/transformation/natural_gas_rigs_in_operation/', object_key=f'natural_gas_rigs_in_operation_{formatted_date}')
    natural_gas_spot_prices_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_json, date_as_index=True)
    heating_oil_spot_prices_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json, date_as_index=True)
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=True)
    natural_gas_rigs_in_operation_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_json, date_as_index=True)

    # Merge dataframes
    curated_training_data_df = EtlTransforms.merge_dataframes(natural_gas_monthly_variables_df=natural_gas_monthly_variables_df, 
    natural_gas_rigs_in_operation_df=natural_gas_rigs_in_operation_df, natural_gas_spot_prices_df=natural_gas_spot_prices_df,
    heating_oil_spot_prices_df=heating_oil_spot_prices_df)

    # Create features from natural gas variables in curated training data
    curated_training_data_df = EiaTransformation.natural_gas_prices_lag(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.heating_oil_to_natural_gas_price_ratio(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.expotential_weighted_natural_gas_price_volatility(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.rolling_average_natural_gas_price(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.rolling_median_natural_gas_price(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.total_consumption_to_total_underground_storage_ratio(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.is_december_or_january(df=curated_training_data_df)

    # Only include records after 2nd most recent date in metadata for natural gas spot prices
    metadata = s3_metadata.get_metadata(folder='full_program/metadata/', object_key='metadata')
    if len(metadata['natural_gas_spot_prices']) > 1:
        date_filter = metadata['natural_gas_spot_prices'][-2]
        date_filter = datetime.strptime(date_filter, '%Y-%m-%d')
        curated_training_data_df = curated_training_data_df[curated_training_data_df.index > date_filter]

    # Drop irrelevant columns
    curated_training_data_df = EtlTransforms.drop_columns(df=curated_training_data_df, columns=['commercial_consumption', 'residential_consumption', 
    'total_underground_storage', 'price_heating_oil ($/GAL)'])

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=curated_training_data_df, folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')

    
    