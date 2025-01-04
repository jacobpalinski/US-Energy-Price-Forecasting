''' Import modules '''
import json
import os
from datetime import datetime, timedelta
import requests
import boto3
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class S3:
    ''' 
    S3 Bucket Class for storage + retrieval of extracted content 
    
    Class Variables
    ---------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    bucket (str): AWS S3 bucket for storage + retrieval
    s3_client (boto3.client): Boto3 S3 connection client

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
    access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    bucket = os.environ.get('S3_BUCKET')

    @classmethod
    def connect(cls) -> None:
        ''' 
        Creates a connection to S3 client using access_key_id and secret_access_key
        '''
        cls.s3_client=boto3.client('s3', aws_access_key_id=cls.access_key_id, 
                                   aws_secret_access_key=cls.secret_access_key)

    @classmethod
    def disconnect(cls) -> None:
        '''
        Disconnects from S3 client
        '''
        del cls.s3_client
    
    @classmethod
    def get_data(cls, folder: str, object_key: str) -> dict:
        '''
        Gets data from S3 bucket for a given folder and object key

        Args:
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
        
        Returns:
            dict: Returns json data in the form of a dictionary object

        '''
        cls.connect()
        object = cls.s3_client.get_object(Bucket=cls.bucket, Key=folder + object_key)
        contents = object['Body'].read().decode('utf-8')
        json_data = json.loads(contents)
        yield json_data
        cls.disconnect()

    @classmethod
    def put_data(cls, data: list, folder: str, object_key: str) -> None:
        '''
        Puts data in S3 bucket as json file under a given folder with a specific object_key

        Args:
            data (list): Data to be put into S3 bucket
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved

        '''
        data_json = json.dumps(data)
        cls.connect()
        cls.s3_client.put_object(Bucket=cls.bucket, Key=folder + object_key, 
                                 Body=data_json, ContentType='application/json')
        cls.disconnect()

class EIA:
    '''
    Class for extracting data from Energy Information Administration API
    
    Class Variables
    ---------------
    api_key (str): API key for making requests
    base_url (str): Base url to be used by all requests

    Methods
    -------
    api_request(endpoint, headers, offset):
        Makes an API request to a specific endpoint of the Energy Information Administration API
    extract(endpoint, parameters, folder, object_key, offset):
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
    
    '''
    api_key = os.environ.get('API_KEY')
    base_url = 'https://api.eia.gov/v2/'

    @classmethod
    def api_request(cls, endpoint: str, headers: dict, offset=0) -> requests.Response:
        '''
        Makes an API request to a specific endpoint of the Energy Information Administration API
        
        Args:
            endpoint (str): Endpoint API request is being made to
            headers (dict): Header values to be passed to the API request
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data

        '''
        url = cls.base_url + endpoint
        params = {'api_key': os.environ.get('API_KEY')}
        headers['offset'] = offset
        headers = {
            'X-Params': json.dumps(headers),
            'Content-Type': 'application/json'
        }
        try: 
            response = requests.get(url, headers=headers,  params=params, timeout=30)
            return response
        except requests.RequestException as e:
            return 'Error occurred', e
    
    @classmethod
    def extract(cls, endpoint: str, headers: dict, folder: str, object_key:str, offset=0) -> None:
        '''
        Extracts data from a request to a specific endpoint and puts data in a S3 endpoint.
        Maybe multiple requests to a specific endpoint as the API can only return 5000 results
        at once hence adjustment of offset maybe necessary
        
        Args:
            endpoint (str): Endpoint API request is being made to
            parameters (dict): Parameters being passed to the API request
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved
            offset (int): Offset in the results. Incremented for results that contain over 5000
            units of data

        '''
        data = []
        while True:
            response = cls.api_request(endpoint=endpoint, headers=headers, offset=offset)
            if response.status_code == 200 and len(response.json()['response']['data']) > 0:
                results = response.json()['response']['data']
                data.extend(results)
                offset += 5000
            else:
                break
        S3.put_data(data=data, folder=folder, object_key=object_key)

class NOAA:
    ''' 
    Class for extracing historical hourly weather data from selected locations from 
    National Oceanic and Atmospheric Administration (NOAA)

    Locations weather data was extracted from:
    Los Angeles, San Diego, San Jose, San Francisco, Sacramento, Miami,
    Tampa, Orlando, Jacksonville, Chicago, St Louis, New Orleans, Baton Rouge,
    Shreveport, Detroit, Grand Rapids, New York, Buffalo, Cleveland,
    Columbus, Cincinnati, Philadelphia, Pittsburgh, Houston, Dallas, San Antonio,
    Austin

    The above locations were located in the states with the highest quantities of
    natural gas consumption across the US. Weather in these locations is going
    to have the greatest impact on price.

    Class Variables
    ---------------
    token (str): Token to be used for API requests
    url (str): Url to be used to extract historical weather data
    locations (dict): Dictionary consisting of stationid : [city, state] key + values

    Methods
    -------
    api_request(parameters):
        Makes an API request to retrieve historical weather data from NOAA API
    extract(parameters, folder, object_key):
        Extract data from request and puts results in an S3 endpoint

    '''
    token = os.environ.get('TOKEN')
    url = 'https://www.ncei.noaa.gov/cdo-web/api/v2/data'
    locations = {'GHCND:USW00023174': ['Los Angeles', 'California'], 'GHCND:USW00023188': ['San Diego', 'California'], 
                 'GHCND:USW00023244': ['San Jose', 'California'], 'GHCND:USW00023234': ['San Francisco', 'California'], 
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
    
    @classmethod
    def api_request(cls, parameters: dict):
        ''' 
        Makes an API request to retrieve historical weather data from NOAA API

        Args:
            parameters (dict): Parameters to be passed to the API request

        '''
        while True:
            try:
                response = requests.get(url = cls.url, headers = {'token': cls.token},
                                        params = parameters, timeout=7)
                if response.status_code == 200:
                    break
            
            except requests.exceptions.Timeout:
                response = requests.get(url = cls.url, headers = {'token': cls.token},
                                        params = parameters, timeout=7)
                    
            except requests.RequestException as e:
                return 'Error occurred', e
            
        return response
    
    @classmethod
    def extract(cls, parameters: dict, folder: str, object_key: str):
        ''' 
        Extract data from requests and puts results in an S3 endpoint.
        
        Args:
            parameters (dict): Parameters to be passed to the API request
            folder (str): S3 folder data is going to be retrieved from
            object_key (str): Name of object being retrieved

        '''
        data = []
        while True:
            response = cls.api_request(parameters)
            results = response.json()['results']
            
            for record in results:
                record['city'] = cls.locations.get(record['station'])[0]
                record['state'] = cls.locations.get(record['station'])[1]
            
            data.extend(results)
            
            startdate = datetime.strptime(parameters['startdate'], '%Y-%m-%d')
            enddate = datetime.strptime(parameters['enddate'], '%Y-%m-%d')
            target = datetime.strptime('2024-04-26', '%Y-%m-%d')
            increment = timedelta(days=6)

            if enddate == datetime(2024, 4, 26):
                break

            elif enddate + increment > target:
                parameters['startdate'] = max(enddate + timedelta(days=1), target - increment).strftime('%Y-%m-%d') # 31/05/2024 original max(enddate, target-increment), adjusted to enddate + timedelta(days=1) due to duplicate records
                parameters['enddate'] = target.strftime('%Y-%m-%d')
            
            else:
                parameters['startdate'] = (startdate + increment).strftime('%Y-%m-%d')
                parameters['enddate'] = (enddate + increment).strftime('%Y-%m-%d')

        S3.put_data(data=data, folder=folder, object_key=object_key)

if __name__ == '__main__':
    EIA.extract(endpoint='natural-gas/pri/fut/data/', headers = {
        'api_key': os.environ.get('API_KEY'),
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': ['RNGWHHD',]
        },
        'start': '1999-01-04',
        'end': '2024-04-26',
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
    }, folder='initial_training/', object_key='natural_gas_spot_prices')
    EIA.extract(endpoint='natural-gas/sum/lsum/data/', headers = {
        'api_key': os.environ.get('API_KEY'),
        'frequency': 'monthly',
        'data': ['value'],
        'facets': {
            'duoarea': [
            'NUS',
            'NUS-Z00'
            ],
         'series': [
            'N3010US2',
            'N3020US2',
            'N5030US2',
            'N5070US2',
            'N5290US2',
            'N9050US2',
            'N9100US2',
            'N9100US3',
            'N9102US2',
            'N9102US3',
            'N9103US2',
            'N9103US3',
            'N9130US2',
            'N9130US3',
            'N9133US2',
            ]
        },
        'start': '1999-01',
        'end': '2024-04',
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
    }, folder='initial_training/', object_key='natural_gas_monthly_variables')
    EIA.extract(endpoint='natural-gas/enr/drill/data/', headers = {
        'api_key': os.environ.get('API_KEY'),
        'frequency': 'monthly',
        'data': ['value'],
        'facets': {
            'series': ['E_ERTRRG_XR0_NUS_C']
            },
        'start': '1999-01',
        'end': '2024-03',
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000,
    }, folder='initial_training/', object_key='natural_gas_rigs_in_operation')
    EIA.extract(endpoint='petroleum/pri/spt/data/', headers = {
        'api_key': os.environ.get('API_KEY'),
        'frequency': 'daily',
        'data': ['value'],
        'facets': {
            'series': [
                'EER_EPD2F_PF4_Y35NY_DPG',
                'RWTC'
            ]
        },
        'start': '1999-01-04',
        'end': '2024-04-26',
        'sort': [{
            'column': 'period',
            'direction': 'asc'
        }],
        'length': 5000
    }, folder='initial_training/', object_key='oil_spot_prices')
    NOAA.extract(parameters = {'datasetid': 'GHCND',
        'datatypeid': ['TMIN', 'TMAX','TAVG', 'SNOW', 'PRCP', 'AWND'],
        'stationid': ['GHCND:USW00023174', 'GHCND:USW00023188', 'GHCND:USW00023244',
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
            'limit': 1000}, folder='initial_training/', object_key='daily_weather')


    


    

    


    

        
            



    

    


    










    
