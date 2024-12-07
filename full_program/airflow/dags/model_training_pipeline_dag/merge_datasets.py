''' Import modules '''
from datetime import datetime
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def merge_dataframes():
    ''' Merges each transformed dataset into a single dataframe '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve each transformed dataset from S3 bucket and convert to dataframe
    daily_weather_json = s3.get_data(folder='full_program/transformation/daily_weather/', object_key=f'daily_weather_{formatted_date}')
    natural_gas_spot_prices_json = s3.get_data(folder='full_program/transformation/natural_gas_spot_prices/', object_key=f'natural_gas_spot_prices_{formatted_date}')
    heating_oil_spot_prices_json = s3.get_data(folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_date}')
    natural_gas_monthly_variables_json = s3.get_data(folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_rigs_in_operation_json = s3.get_data(folder='full_program/transformation/natural_gas_rigs_in_operation/', object_key=f'natural_gas_rigs_in_operation_{formatted_date}')
    
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=True)
    natural_gas_spot_prices_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_json, date_as_index=True)
    heating_oil_spot_prices_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json, date_as_index=True)
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=True)
    natural_gas_rigs_in_operation_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_json, date_as_index=True)

    # Merge dataframes into a single dataframe
    merged_df = EtlTransforms.merge_dataframes(daily_weather_df=daily_weather_df, natural_gas_monthly_variables_df=natural_gas_monthly_variables_df,
    natural_gas_rigs_in_operation_df=natural_gas_rigs_in_operation_df, natural_gas_spot_prices_df=natural_gas_spot_prices_df,
    heating_oil_spot_prices_df=heating_oil_spot_prices_df)

    # Reset index so date column is stored as json
    merged_df = merged_df.reset_index()

    # Convert date from timestamp to string
    merged_df['date'] = merged_df['date'].dt.strftime('%Y-%m-%d')
    
    # Put data in S3
    s3.put_data(data=merged_df, folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')