''' Import modules '''
from datetime import datetime
import pandas as pd
from dags.utils.config import *
from dags.utils.aws import *
from dags.transformation.etl_transforms import EtlTransforms

# Todays date
today = datetime.now()
formatted_date = today.strftime('%Y%m%d')

# Instantiate classes for Config, S3, S3Metadata
config = Config()
s3 = S3(config=config)

# Retrieve training data for 2024-12-26 (adjust date if going to update imputation dataframe in future)
training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key='curated_training_data_20241226')
training_data_df = EtlTransforms.json_to_df(data=training_data_json, date_as_index=False)

# Convert date column to datetime
training_data_df['date'] = pd.to_datetime(training_data_df['date'])

# Calculate median for each day of a given year for weather variables
training_data_df['month'] = training_data_df['date'].dt.month
training_data_df['week'] = training_data_df['date'].dt.isocalendar().week
training_data_df['day'] = training_data_df['date'].dt.day

daily_weather_modelling_imputation_df = training_data_df.groupby(['month', 'week', 'day'])[['min_tavg', 'max_tavg', 'max_abs_tavg_diff', 
'max_abs_tavg_diff_relative_to_daily_median', 'hdd_max', 'cdd_max', 'wci_sum', 'snow_sum']].median().reset_index()

# Store data in S3 bucket
s3.put_data(data=daily_weather_modelling_imputation_df, folder='full_program/curated/imputation/', object_key=f'daily_weather_modelling_imputation_base_{formatted_date}')




