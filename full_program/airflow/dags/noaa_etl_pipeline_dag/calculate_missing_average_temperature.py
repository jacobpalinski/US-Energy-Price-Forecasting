''' Import modules '''
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dags.extraction.noaa_api import *
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.noaa_api_transformation import NoaaTransformation

def calculate_missing_average_temperature():
    ''' Calculate missing average temperature values in extracted NOAA weather data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    daily_weather_json = s3.get_data(folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)

    # Split dataframe into 20 chunks
    chunks = np.array_split(daily_weather_df, 20)

    # List to store processed chunks
    processed_chunks = []

    # Calculate tavg for columns where it is missing in each chunk
    for chunk in chunks:
        print(f'Processing chunk with {len(chunk)} rows')
        transformed_chunk = NoaaTransformation.calculate_missing_tavg(df=chunk)
        processed_chunks.append(transformed_chunk)
    
    # Concatenate all processed chunks into a single dataframe
    daily_weather_df = pd.concat(processed_chunks, ignore_index=True)
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')