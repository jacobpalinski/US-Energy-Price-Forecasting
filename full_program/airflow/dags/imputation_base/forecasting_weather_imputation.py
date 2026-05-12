# Import modules
from datetime import datetime
import pandas as pd
from dags.utils.config import Config
from dags.utils.aws import S3, S3Metadata
from dags.transformation.etl_transforms import EtlTransforms

# Todays timestamp
today = datetime.now()
timestamp_str = today.strftime('%Y%m%d%H%M%S')

# Instantiate classes for Config, S3, S3Metadata
config = Config()
s3 = S3(config=config)
s3_metadata = S3Metadata(config=config)

# Retrieve training data for 2024-12-26 (module only run once hence hardcoding s3_key)
training_data_json = s3.get_data(s3_key=config.daily_weather_modelling_imputation_base_curated_training_data_s3_key)
training_data_df = EtlTransforms.json_to_df(data=training_data_json, date_as_index=False)

# Convert date column to datetime
training_data_df['date'] = pd.to_datetime(training_data_df['date'])

# Calculate median for each day of a given year for weather variables
training_data_df['month'] = training_data_df['date'].dt.month
training_data_df['week'] = training_data_df['date'].dt.isocalendar().week
training_data_df['day'] = training_data_df['date'].dt.day

daily_weather_modelling_imputation_df = training_data_df.groupby(['month', 'week', 'day'])[['min_tavg', 'max_tavg', 'max_abs_tavg_diff', 
'max_abs_tavg_diff_relative_to_daily_median', 'hdd_max', 'cdd_max', 'wci_sum', 'snow_sum']].median().reset_index()

# Store data in S3 bucket to be used for imputation as part of forecasting
s3.put_data(data=daily_weather_modelling_imputation_df, s3_key=f'full_program/curated/imputation/daily_weather_modelling_imputation_base_{timestamp_str}.json')
s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='daily_weather_modelling_imputation_base', latest_transformed_file_path=f'full_program/curated/imputation/daily_weather_modelling_imputation_base_{timestamp_str}.json')




