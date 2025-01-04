''' Import modules '''
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import os
import requests
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import *
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class EIA:
    '''
    Class for extracting data from Energy Information Administration API
    
    Instance Variables
    ------------------
    api_key (str): API key for making requests
    base_url (str): Base url to be used by all requests
    s3 (S3): s3 bucket where data is to be extracted to
    s3 (S3Metadata): s3 metadata storage location

    Methods
    -------
    api_request(cls, endpoint, headers, metadata_folder, metadata_object_key, metadata_dataset_key, start_date_if_none, is_monthly, offset):
        Makes an API request to a specific endpoint of the Energy Information Administration API
    get_max_date(cls, data, is_monthly):
        Retrieves latest end date from data extracted to be logged in metadata
    extract(cls, endpoint, headers, folder, object_key, metadata_folder, metadata_object_key, metadata_dataset_key, start_date_if_none, is_monthly, offset):
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
    '''
    def __init__(self, config: Config, s3: S3, s3_metadata: S3Metadata):
        self.eia_api_key = config.eia_api_key
        self.base_url = 'https://api.eia.gov/v2/'
        self.s3 = s3
        self.s3_metadata = s3_metadata

    def api_request(self, endpoint: str, headers: dict, metadata_folder: str, metadata_object_key: str, metadata_dataset_key: str, start_date_if_none: str, is_monthly: bool, offset=0) -> requests.Response:
        '''
        Makes an API request to a specific endpoint of the Energy Information Administration API
        
        Args:
            endpoint (str): Endpoint API request is being made to
            headers (dict): Header values to be passed to the API request
            metadata_folder (str): Metadata folder where latest end date for data extraction of given dataset is retrieved from
            metadata_object_key (str): Metadata object where latest end date is being retrieved from
            metadata_dataset_key (str): Dataset key from metadata where latest end date is being retrieved for
            start_date_if_none (str): Date to be used for start_date key in headers if there are no dates for a given metadata dataset key
            is_monthly (bool): Used to identify whether data is extract has daily or monthly granularity
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data
        
        Returns:
            requests.Response: Response object from API request
        '''
        # Create url for data extraction
        url = self.base_url + endpoint

        # Initialise api key as parameter
        params = {'api_key': self.eia_api_key}

        # Retrieve latest end date. Latest end date to be used as start date in api request
        latest_end_date = self.s3_metadata.get_latest_end_date(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key) # Varies depending on metadata for given dataset
        if latest_end_date is None:
            start_date = start_date_if_none
        else:
            latest_end_date_datetime = datetime.strptime(latest_end_date, '%Y-%m-%d')
            if is_monthly is True:
                latest_end_date_plus_one = latest_end_date_datetime + relativedelta(months=1)
            else:
                latest_end_date_plus_one = latest_end_date_datetime + timedelta(days=1)
            start_date = latest_end_date_plus_one.strftime('%Y-%m-%d')
        
        # Set headers
        headers['start'] = start_date
        headers['offset'] = offset
        headers = {
            'X-Params': json.dumps(headers),
            'Content-Type': 'application/json'
        }

        # Make request
        try: 
            response = requests.get(url, headers=headers,  params=params, timeout=30)
            return response
        except requests.RequestException as e:
            return 'Error occurred', e
    
    def get_max_date(self, data: list, is_monthly: bool) -> str:
        ''' 
        Retrieves latest end date from data extracted to be logged in metadata
        Args:
            data (list): Records in extracted data
            is_monthly (bool): Used to identify whether data is extract has daily or monthly granularity

        Returns:
            str: Latest end date in string format 
        '''
        if not data:
            return None
        elif is_monthly is False: 
            dates = [datetime.strptime(item['period'], '%Y-%m-%d') for item in data]
        else:
            dates = [datetime.strptime(item['period'], '%Y-%m') for item in data]
        
        max_date = max(dates)
        max_date_str = max_date.strftime('%Y-%m-%d') # Conversion required for monthly date granularity
        return max_date_str
        
    def extract(self, endpoint: str, headers: dict, folder: str, object_key: str, metadata_folder: str, 
        metadata_object_key: str, metadata_dataset_key: str, start_date_if_none: str, is_monthly: bool, offset=0) -> None:
        '''
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
        
        Args:
            endpoint (str): Endpoint API request is being made to
            headers (dict): Parameters being passed to the API request
            folder (str): S3 folder data is going to be ingressed into
            object_key (str): Name of object being ingressed into S3 bucket
            metadata_folder (str): Metadata folder where latest end date for data extraction of given dataset is retrieved from
            metadata_object_key (str): Metadata object where latest end date is being retrieved from
            metadata_dataset_key (str): Dataset key from metadata where latest end date is being retrieved for
            start_date_if_none (str): Date to be used for start_date key in headers if there are no dates for a given metadata dataset key
            is_monthly (bool): Used to identify whether data is extract has daily or monthly granularity
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data
        '''
        # Initialise data variable to store result from api_request
        data = []

        # Append results to data as long as data exists for a given API request
        while True:
            response = self.api_request(endpoint=endpoint, headers=headers, metadata_folder=metadata_folder, metadata_object_key=metadata_object_key,
                                       metadata_dataset_key = metadata_dataset_key, start_date_if_none=start_date_if_none,
                                        is_monthly=is_monthly, offset=offset)
            if response.status_code == 200 and len(response.json()['response']['data']) > 0:
                results = response.json()['response']['data']
                data.extend(results)
                offset += 5000
            else:
                break
        
        # Retrieve maximum date in records extracted
        max_date = self.get_max_date(data, is_monthly=is_monthly)
        if max_date is None:
            return
        # Append results to S3 bucket folder and update latest date extracted from a given url in metadata
        else:
            self.s3.put_data(data=data, folder=folder, object_key=object_key)
            self.s3_metadata.update_metadata(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key, new_date=max_date)