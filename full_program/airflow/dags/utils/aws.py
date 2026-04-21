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
    get_data(s3_key):
        Gets data from S3 bucket for a given folder and object key
    put_data(data, s3_key):
        Puts data in S3 bucket as json file under a given folder with a specific object_key
        
    '''
    def __init__(self, config: Config):
        self.access_key_id = config.access_key_id
        self.secret_access_key = config.secret_access_key
        self.bucket = config.bucket
        self.s3_client=boto3.client('s3', aws_access_key_id=self.access_key_id, 
                                   aws_secret_access_key=self.secret_access_key)
    
    def get_data(self, s3_key: str) -> dict:
        '''
        Gets data from S3 bucket for a given folder and object key

        Args:
            s3_key (str): The S3 key for the object to retrieve
        
        Returns:
            dict: Returns json data in the form of a dictionary object

        '''
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            contents = response['Body'].read().decode('utf-8')
            json_data = json.loads(contents)
            return json_data
        
        except ClientError as e:
            print(f'Error retrieving metadata: {e}')
            return {}

    def put_data(self, data: list, s3_key: str) -> None:
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
        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, 
                                  Body=data_json, ContentType='application/json')

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
    get_metadata(s3_key: str) -> dict:
        Retrieves metadata from S3, including multiple dates for each dataset 
    update_metadata(s3_key: str, dataset_key: str, **kwargs) -> None:
        Updates the metadata in S3 with a new end date for a given dataset
    
    '''
    def get_metadata(self, s3_key: str) -> dict:
        ''' 
        Retrieves metadata from S3, including multiple dates for each dataset 
    
        Args:
            s3_key (str): The S3 key for the metadata file
    
        Returns:
            dict: A dictionary where keys are dataset identifiers and values are a dictionary consisting of latest_start_date, latest_end_date,
            latest_extracted_file_path and latest_transformed_file_path.
    
        '''
        metadata = self.get_data(s3_key=s3_key)
        return metadata

    def update_metadata(self, s3_key, dataset_key, **kwargs) -> None:
        '''
        Updates the metadata in S3 with a new end date for a given dataset.

        Args:
            s3_ket (str): The S3 key where the metadata is stored
            dataset_key (str): The identifier of the dataset for which metadata is being updated
            **kwargs: Additional keyword arguments to handle latest start date, latest end date, latest extracted file path and latest transformed file path
        '''
        metadata = self.get_metadata(s3_key=s3_key)
        if dataset_key not in metadata:
            metadata[dataset_key] = {}
        for key, value in kwargs.items():
            metadata[dataset_key][key] = value
        self.put_data(data=metadata, s3_key=s3_key)

class SNSNotifier:
    ''' 
    Class for sending notifications using AWS SNS service
    
    Instance Variables
    ------------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    topic_arn (str): ARN of the SNS topic to which notifications will be sent

    Methods
    -------
    connect:
        Creates a connection to SNS client using access_key_id and secret_access_key
    '''
    def __init__(self, config: Config):
        self.access_key_id = config.access_key_id
        self.secret_access_key = config.secret_access_key
        self.region = config.region
        self.topic_arn = config.topic_arn
        self.sns_client = boto3.client('sns', aws_access_key_id=self.access_key_id, 
                                        aws_secret_access_key=self.secret_access_key,
                                        region_name=self.region)
    
    def build_message(self, context: dict) -> json:
        '''
        Builds a message payload for SNS notification based on the Airflow context.
        Args:
            context (dict): Airflow context dictionary containing information about the DAG run, task instance, and any exceptions.
        Returns:
            json: A JSON-formatted object containing relevant information for the notification.
        '''
        # Retrieves relevant information from Airflow context to include in notification message
        task_instance = context.get("task_instance")
        dag = context.get("dag")
        exception = context.get("exception")

        # Create payload information to be sent in SNS notification
        payload = {
            "dag_id": dag.dag_id if dag else None,
            "task_id": task_instance.task_id if task_instance else None,
            "execution_date": str(context.get("execution_date")),
            "log_url": task_instance.log_url if task_instance else None,
            "exception": str(exception) if exception else None,
        }

        # Json payload to be sent in SNS notification
        return json.dumps(payload, indent=2)

    def send(self, context: dict):
        '''
        Sends an SNS notification with relevant information about the Airflow task failure.
        Args:
            context (dict): Airflow context dictionary containing information about the DAG run, task instance, and any exceptions.
        '''
        # Retrieves relevant information from Airflow context to include in notification message
        dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"

        # Build message payload for SNS notification
        message = self.build_message(context)

        # Send SNS notification
        self.sns_client.publish(
            TopicArn=self.topic_arn,
            Subject=f"Airflow Alert: {dag_id}.{task_id} Failed",
            Message=message,
        )
    
    def __call__(self, context):
        self.send(context)
    

    




















    


    

    


