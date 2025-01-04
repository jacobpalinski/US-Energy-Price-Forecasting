''' Import modules '''
from datetime import datetime, timedelta
import os
import requests 
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import *
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class NOAA:
    ''' 
    Class for extracing historical hourly weather data from selected locations from 
    National Oceanic and Atmospheric Administration (NOAA)

    Locations weather data was extracted from:
    Los Angeles, San Diego, San Francisco, Sacramento, Miami,
    Tampa, Orlando, Jacksonville, Chicago, St Louis, New Orleans, Baton Rouge,
    Shreveport, Detroit, Grand Rapids, New York, Buffalo, Cleveland,
    Columbus, Cincinnati, Philadelphia, Pittsburgh, Houston, Dallas, San Antonio,
    Austin

    The above locations were located in the states with the highest quantities of
    natural gas consumption across the US. Weather in these locations is going
    to have the greatest impact on price.

    Instance Variables
    ------------------
    token (str): Token to be used for API requests
    url (str): Url to be used to extract historical weather data
    locations (dict): Dictionary consisting of stationid : [city, state] key + values
    s3 (S3): s3 bucket where data is to be extracted to
    s3 (S3Metadata): s3 metadata storage location

    Methods
    -------
    api_request(cls, parameters):
        Makes an API request to retrieve historical weather data from NOAA API
    get_max_date(cls, data):
        Retrieves latest end date from data extracted to be logged in metadata
    extract(cls, parameters, folder, object_key):
        Extract data from request and puts results in an S3 endpoint
    '''
    def __init__(self, config: Config, s3: S3, s3_metadata: S3Metadata):
        self.token = config.token
        self.base_url = 'https://www.ncei.noaa.gov/cdo-web/api/v2/data'
        self.locations = {'GHCND:USW00023174': ['Los Angeles', 'California'], 'GHCND:USW00023188': ['San Diego', 'California'], 
        'GHCND:USW00023234': ['San Francisco', 'California'], 
        'GHCND:USW00023232': ['Sacramento', 'California'], 'GHCND:USW00012839':['Miami', 'Florida'], 
        'GHCND:USW00012842': ['Tampa', 'Florida'], 'GHCND:USW00012815':['Orlando', 'Florida'], 
        'GHCND:USW00013889': ['Jacksonville', 'Florida'], 'GHCND:USW00094846': ['Chicago', 'Illinois'], 
        'GHCND:USW00013994': ['St Louis', 'Illinois'], 'GHCND:USW00012916': ['New Orleans', 'Louisiana'], 
        'GHCND:USW00013970': ['Baton Rouge', 'Louisiana'], 'GHCND:USW00013957': ['Shreveport', 'Louisiana'], 
        'GHCND:USW00094847': ['Detroit', 'Michigan'], 'GHCND:USW00094860': ['Grand Rapids', 'Michigan'], 
        'GHCND:USW00014734': ['New York', 'New York'], 'GHCND:USW00014733': ['Buffalo', 'New York'], 
        'GHCND:USW00014820': ['Cleveland', 'Ohio'], 'GHCND:USW00014821': ['Columbus', 'Ohio'], 
        'GHCND:USW00093814': ['Cincinnati', 'Ohio'], 'GHCND:USW00013739': ['Philadelphia', 'Pennsylvania'], 
        'GHCND:USW00094823': ['Pittsburgh', 'Pennsylvania'], 'GHCND:USW00012960': ['Houston', 'Texas'], 
        'GHCND:USW00013960': ['Dallas', 'Texas'], 'GHCND:USW00012921': ['San Antonio', 'Texas'], 
        'GHCND:USW00013904': ['Austin', 'Texas']}
        self.s3 = s3
        self.s3_metadata = s3_metadata
    
    def api_request(self, parameters: dict) -> requests.Response:
        ''' 
        Makes an API request to retrieve historical weather data from NOAA API

        Args:
            parameters (dict): Parameters to be passed to the API request
        
        Returns:
            requests.Response: Response object from API request
        '''
        # Max attempts variable created to repeat requests when a given request fails
        max_attempts = 3
        attempt = 0

        # Repeat requests when number of attempts < max_attempts
        while attempt < max_attempts:
            try:
                response = requests.get(url = self.base_url, headers = {'token': self.token},
                                        params = parameters, timeout=7)
                if response.status_code == 200:
                    return response
            
            except requests.exceptions.Timeout:
                attempt += 1
                    
            except requests.RequestException as e:
                return 'Error occurred', e
    
    def get_max_date(self, data: list) -> str:
        ''' 
        Retrieves latest end date from data extracted to be logged in metadata
        Args:
            data (list): Records in extracted data
        
        Returns:
            str: Latest end date in string format 
        '''
        if not data:
            return None
        else: 
            dates = [datetime.strptime(item['date'], '%Y-%m-%dT%H:%M:%S') for item in data]
            max_date = max(dates)
            max_date_str = max_date.strftime('%Y-%m-%d')
            return max_date_str
    
    def extract(self, parameters: dict, folder: str, object_key: str, metadata_folder: str, 
        metadata_object_key: str, metadata_dataset_key: str, start_date_if_none: str) -> None:
        ''' 
        Extract data from requests and puts results in an S3 endpoint.
        
        Args:
            parameters (dict): Parameters to be passed to the API request
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
            metadata_folder (str): Metadata folder where latest end date for data extraction of given dataset is retrieved from
            metadata_object_key (str): Metadata object where latest end date is being retrieved from
            metadata_dataset_key (str): Dataset key from metadata where latest end date is being retrieved for
            start_date_if_none (str): Date to be used for start_date key in headers if there are no dates for a given metadata dataset key
        '''
        # Initialise data variable to store result from api_request
        data = []
        
        # Retrieve lastest end date. Latest end date used as start date in API request
        latest_end_date = self.s3_metadata.get_latest_end_date(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key)
        increment = timedelta(days=6)
        if latest_end_date is None:
            start_date = start_date_if_none
        else:
            latest_end_date_datetime = datetime.strptime(latest_end_date, '%Y-%m-%d')
            latest_end_date_plus_one = latest_end_date_datetime + timedelta(days=1)
            start_date = latest_end_date_plus_one.strftime('%Y-%m-%d')
        parameters['startdate'] = start_date
        enddate = datetime.strptime(start_date, '%Y-%m-%d')
        parameters['enddate'] = (enddate + increment).strftime('%Y-%m-%d')

        # Append results to data as long as data exists for a given API request
        while True:
            print(parameters['startdate'])
            response = self.api_request(parameters=parameters)
            results = response.json().get('results', [])

            if not results:
                break
            
            for record in results:
                record['city'] = self.locations.get(record['station'])[0]
                record['state'] = self.locations.get(record['station'])[1]
            
            data.extend(results)
            
            startdate = datetime.strptime(parameters['startdate'], '%Y-%m-%d')
            enddate = datetime.strptime(parameters['enddate'], '%Y-%m-%d')
                
            parameters['startdate'] = (startdate + increment).strftime('%Y-%m-%d')
            parameters['enddate'] = (enddate + increment).strftime('%Y-%m-%d')
        
        # Retrieve maximum date in records extracted
        max_date = self.get_max_date(data)
        if max_date is None:
            return
        # Append results to S3 bucket folder and update latest date extracted from a given url in metadata
        else:
            self.s3.put_data(data=data, folder=folder, object_key=object_key)
            self.s3_metadata.update_metadata(folder=metadata_folder, object_key=metadata_object_key, dataset_key=metadata_dataset_key, new_date=max_date)

