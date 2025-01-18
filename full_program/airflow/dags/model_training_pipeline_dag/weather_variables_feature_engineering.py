''' Import modules '''
from datetime import datetime
import pandas as pd
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.noaa_api_transformation import NoaaTransformation
from dags.transformation.etl_transforms import EtlTransforms

def weather_variables_feature_engineering():
    ''' Function that engineers features from weather variables and merges with curated natural gas data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve daily_weather data from S3 folder
    daily_weather_json = s3.get_data(folder='full_program/transformation/daily_weather/', object_key=f'daily_weather_{formatted_date}')
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=True)

    # Retrieve curated training data from S3 folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Create features from natural gas variables in curated training data
    daily_weather_df = NoaaTransformation.maximum_hdd(df=daily_weather_df)
    daily_weather_df = NoaaTransformation.maximum_cdd(df=daily_weather_df)
    daily_weather_df = NoaaTransformation.wci_sum(df=daily_weather_df)
    daily_weather_df = NoaaTransformation.snow_sum(df=daily_weather_df)
    daily_weather_df = NoaaTransformation.min_and_max_average_temperature(df=daily_weather_df)
    daily_weather_df = NoaaTransformation.max_abs_tavg_diff(df=daily_weather_df)
    daily_weather_df = NoaaTransformation.max_abs_tavg_diff_relative_to_daily_median(df=daily_weather_df)

    # Drop irrelevant columns
    daily_weather_df = EtlTransforms.drop_columns(df=daily_weather_df, columns=['city', 'state', 'quarter', 'tmin', 'tmax', 'tavg', 'snow', 'awnd'])

    # Drop duplicated records
    daily_weather_df = daily_weather_df.drop_duplicates()

    # Merge daily weather dataframe with curated training data
    curated_training_data_df = pd.merge(curated_training_data_df, daily_weather_df, left_index=True, right_index=True, how='left')

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=curated_training_data_df, folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')