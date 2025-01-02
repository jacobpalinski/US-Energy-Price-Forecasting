''' Import modules '''
from datetime import datetime
from dags.utils.config import *
from dags.utils.aws import *
from dags.extraction.noaa_api import NOAA
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.noaa_api_transformation import NoaaTransformation

# Todays date
today = datetime.now()
formatted_date = today.strftime('%Y%m%d')

# Instantiate classes for Config, S3, S3Metadata and NOAA
config = Config()
s3 = S3(config=config)
s3_metadata = S3Metadata(config=config)
noaa = NOAA(config=config, s3=s3, s3_metadata=s3_metadata)

# Extract weather data from NOAA API
parameters = {'datasetid': 'GHCND',
'datatypeid': ['TMIN', 'TMAX','TAVG', 'SNOW', 'AWND'],
'stationid': ['GHCND:USW00023174', 'GHCND:USW00023188',
'GHCND:USW00023234', 'GHCND:USW00023232', 'GHCND:USW00012839',
'GHCND:USW00012842', 'GHCND:USW00012815', 'GHCND:USW00013889',
'GHCND:USW00094846', 'GHCND:USW00013994', 'GHCND:USW00012916',
'GHCND:USW00013970', 'GHCND:USW00013957', 'GHCND:USW00094847',
'GHCND:USW00094860', 'GHCND:USW00014734', 'GHCND:USW00014733',
'GHCND:USW00014820', 'GHCND:USW00014821', 'GHCND:USW00093814',
'GHCND:USW00013739', 'GHCND:USW00094823', 'GHCND:USW00012960',
'GHCND:USW00013960', 'GHCND:USW00012921', 'GHCND:USW00013904'],
'startdate': '1999-01-04',
'enddate': '1999-01-09',
'units': 'metric',
'limit': 1000}

noaa.extract(parameters=parameters, folder='full_program/extraction/daily_weather/', 
object_key = f'daily_weather_{formatted_date}', metadata_folder='metadata/', 
metadata_object_key='metadata', metadata_dataset_key='daily_weather', 
start_date_if_none='1999-01-04')
daily_weather_json = s3.get_data(folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')
daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)
daily_weather_df = NoaaTransformation.modify_date(df=daily_weather_df)
imputation_df = NoaaTransformation.imputation_df(df=daily_weather_df)
s3.put_data(data=imputation_df, folder='full_program/extraction/imputation/', object_key=f'imputation_base_{formatted_date}')









