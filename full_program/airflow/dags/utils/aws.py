''' Import modules '''
import json
import os
import math
from datetime import datetime
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from dags.utils.config import *

# Import environment variables
load_dotenv()

class S3:
    ''' 
    S3 Bucket Class for storage + retrieval of extracted content 
    
    Instance Variables
    ------------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    bucket (str): AWS S3 bucket for storage + retrieval

    Methods
    -------
    connect(cls):
        Creates a connection to S3 client using access_key_id and secret_access_key
    disconnect(cls):
        Disconnects from S3 client
    get_data(cls, folder, object_key):
        Gets data from S3 bucket for a given folder and object key
    put_data(cls, data, folder, object_key):
        Puts data in S3 bucket as json file under a given folder with a specific object_key
        
    '''
    def __init__(self, config: Config):
        self.access_key_id = config.access_key_id
        self.secret_access_key = config.secret_access_key
        self.bucket = config.bucket

    def connect(self) -> None:
        ''' 
        Creates a connection to S3 client using access_key_id and secret_access_key
        '''
        self.s3_client=boto3.client('s3', aws_access_key_id=self.access_key_id, 
                                   aws_secret_access_key=self.secret_access_key)

    def disconnect(self) -> None:
        '''
        Disconnects from S3 client
        '''
        del self.s3_client
    
    def get_data(self, folder: str, object_key: str) -> dict:
        '''
        Gets data from S3 bucket for a given folder and object key

        Args:
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
        
        Returns:
            dict: Returns json data in the form of a dictionary object

        '''
        try:
            self.connect()
            response = self.s3_client.get_object(Bucket=self.bucket, Key=folder + object_key)
            contents = response['Body'].read().decode('utf-8')
            json_data = json.loads(contents)
            return json_data
        
        except ClientError as e:
            print(f'Error retrieving metadata: {e}')
            return {}

        finally:
            self.disconnect()

    def put_data(self, data: list, folder: str, object_key: str) -> None:
        '''
        Puts data in S3 bucket as json file under a given folder with a specific object_key

        Args:
            data (list): Data to be put into S3 bucket
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved

        '''
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')

        data_json = json.dumps(data)
        self.connect()
        self.s3_client.put_object(Bucket=self.bucket, Key=folder + object_key, 
        Body=data_json, ContentType='application/json')
        self.disconnect()

class S3Metadata(S3):
    ''' 
    Class for updating and retrieving metadata from S3 folder 
        
    Instance Variables
    ---------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    bucket (str): AWS S3 bucket for storage + retrieval

    Methods
    --------------
    get_metadata(cls):
        Retrieves metadata from S3, including multiple dates for each dataset 
    get_latest_end_date(cls):
        Retrieves the latest end date for a given dataset inside metadata file
    update_metadata(cls):
        Updates the metadata in S3 with a new end date for a given dataset.
    
    '''
    def get_metadata(self, folder: str, object_key: str) -> dict:
        ''' 
        Retrieves metadata from S3, including multiple dates for each dataset 
    
        Args:
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved 
    
        Returns:
            dict: A dictionary where keys are dataset identifiers and values are lists of dates.
    
        '''
        metadata = self.get_data(folder=folder, object_key=object_key)
        return metadata

    def get_latest_end_date(self, folder: str, object_key: str, dataset_key: str) -> str:
        '''
        Retrieves the latest end date for a given dataset inside metadata file

        Args:
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
            dataset_key (str): The identifier of the dataset
        
        Returns:
            str: The latest end date in 'YYYY-MM-DD' format or None if no date exists
        '''
        metadata = self.get_metadata(folder=folder, object_key=object_key)
        dates = metadata.get(dataset_key, [])
        if dates:
            max_date = max(dates, key=lambda d: datetime.strptime(d, '%Y-%m-%d'))
            return max_date
        return None

    def update_metadata(self, folder: str, object_key: str, dataset_key: str, new_date: str) -> None:
        '''
        Updates the metadata in S3 with a new end date for a given dataset.

        Args:
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
            dataset_key (str): The identifier for the dataset
            new_date (str): The new end date to be added in 'YYYY-MM-DD' format.
        '''
        metadata = self.get_metadata(folder=folder, object_key=object_key)
        if dataset_key not in metadata:
            metadata[dataset_key] = []
        if new_date not in metadata[dataset_key]:
            metadata[dataset_key].append(new_date)
        metadata[dataset_key].sort() # Sorts dates for given metadata key
        self.put_data(data=metadata, folder='full_program/metadata/', object_key=object_key)
    




















    


    

    


