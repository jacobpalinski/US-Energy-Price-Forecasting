''' Import modules '''
from datetime import datetime
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.noaa_api_transformation import NoaaTransformation
from dags.transformation.etl_transforms import EtlTransforms

def weather_variables_feature_engineering():
    ''' Function that engineers features from weather variables in curated training data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3 folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Create features from natural gas variables in curated training data
    curated_training_data_df = NoaaTransformation.maximum_hdd(df=curated_training_data_df)
    curated_training_data_df = NoaaTransformation.maximum_cdd(df=curated_training_data_df)
    curated_training_data_df = NoaaTransformation.wci_sum(df=curated_training_data_df)
    curated_training_data_df = NoaaTransformation.snow_sum(df=curated_training_data_df)
    curated_training_data_df = NoaaTransformation.min_and_max_average_temperature(df=curated_training_data_df)
    curated_training_data_df = NoaaTransformation.max_abs_tavg_diff(df=curated_training_data_df)
    curated_training_data_df = NoaaTransformation.max_abs_tavg_diff_relative_to_daily_median(df=curated_training_data_df)

    # Drop irrelvant columns
    curated_training_data_df = EtlTransforms.drop_columns(df=curated_training_data_df, columns=['city', 'state', 'quarter', 'tmin', 'tmax', 'tavg', 'snow', 'awnd'])

    # Drop duplicated records
    curated_training_data_df = curated_training_data_df.drop_duplicates()

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=curated_training_data_df, folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')