# Import modules
import os
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class Config:
    ''' 
    Class which initialises environment variables used by S3, S3Metadata, EIA and NOAA classes 
    
    Instance Variables
    ------------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    region (str): AWS region
    bucket (str): AWS S3 bucket for storage + retrieval
    topic_arn (str): AWS SNS topic arn for sending notifications
    eia_api_key (str): API key for retrieving data from EIA API
    token (str): Token for retrieving data from NOAA API
    daily_weather_modelling_imputation_base_curated_training_data_s3_key (str): S3 key for retrieving daily_weather_modelling_imputation_base dataset
    '''
    def __init__(self):
        self.access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.region = os.environ.get('AWS_REGION')
        self.bucket = os.environ.get('S3_BUCKET')
        self.topic_arn = os.environ.get('TOPIC_ARN')
        self.eia_api_key = os.environ.get('API_KEY')
        self.token = os.environ.get('TOKEN')
        self.daily_weather_modelling_imputation_base_curated_training_data_s3_key = 'full_program/curated/training_data/curated_training_data_20241226.json'