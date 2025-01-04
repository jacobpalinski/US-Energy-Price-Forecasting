''' Import modules '''
import json
import pytest
import requests
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
from dags.extraction.eia_api import EIA
from dags.extraction.noaa_api import NOAA
from dags.fixtures.fixtures import mock_environment_variables, mock_boto3_client, mock_eia, mock_noaa, mock_requests_get, mock_get_latest_end_date, mock_update_metadata, mock_eia_headers, \
mock_noaa_parameters, mock_natural_gas_spot_prices_response, mock_natural_gas_monthly_variables_response, mock_noaa_daily_weather_data_response

class TestEIA:
    ''' Test class for testing EIA class '''
    def test_eia_api_request_success_with_latest_end_date_default_offset(self, mock_environment_variables, mock_eia, mock_requests_get, mock_get_latest_end_date, mock_eia_headers, mock_natural_gas_spot_prices_response):
        ''' Test api_request method of EIA class where latest end date is specified for a given dataset key '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_natural_gas_spot_prices_response).encode('utf-8')
        mock_requests_get.return_value = mock_response
        
        headers = mock_eia_headers
        endpoint = 'natural-gas/pri/fut/data/'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = ''
        
        response = mock_eia.api_request(endpoint=endpoint,
        headers = headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none)
        
        mock_requests_get.assert_called_once_with('https://api.eia.gov/v2/' + endpoint,
            headers={'X-Params': json.dumps({
                'api_key': 'api_key',
                'frequency': 'daily',
                'data': ['value'],
                'facets': {
                    'series': ['RNGWHHD',]
                },
                'sort': [{
                'column': 'period',
                'direction': 'asc'
                }],
                'length': 5000,
                'start': '1999-01-04',
                'offset': 0}),
                'Content-Type': 'application/json'},
            params = {'api_key': 'api_key'},
            timeout=30
        )
        assert response.status_code == 200
        assert response.json() == mock_natural_gas_spot_prices_response
    
    def test_eia_api_request_success_with_non_default_offset(self, mock_environment_variables, mock_eia, mock_requests_get, mock_get_latest_end_date, mock_eia_headers, mock_natural_gas_spot_prices_response):
        ''' Test api_request method of EIA class where latest end date is specified for a given dataset key and non-default offset '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_natural_gas_spot_prices_response).encode('utf-8')
        mock_requests_get.return_value = mock_response

        endpoint = 'natural-gas/pri/fut/data/'
        headers = mock_eia_headers
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = ''
        offset = 5000
        
        response = mock_eia.api_request(endpoint=endpoint,
        headers = headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none,
        offset=offset)
        
        mock_requests_get.assert_called_once_with('https://api.eia.gov/v2/' + endpoint,
            headers={'X-Params': json.dumps({
                'api_key': 'api_key',
                'frequency': 'daily',
                'data': ['value'],
                'facets': {
                    'series': ['RNGWHHD',]
                },
                'sort': [{
                'column': 'period',
                'direction': 'asc'
                }],
                'length': 5000,
                'start': '1999-01-04',
                'offset': offset}),
                'Content-Type': 'application/json'},
            params = {'api_key': 'api_key'},
            timeout=30
        )
        assert response.status_code == 200
        assert response.json() == mock_natural_gas_spot_prices_response
    
    def test_eia_api_request_with_latest_end_date_none(self, mock_environment_variables, mock_eia, mock_requests_get, mock_get_latest_end_date, mock_eia_headers, mock_natural_gas_spot_prices_response):
        ''' Test api_request method of EIA class where latest end date is specified for a given dataset key and non-default offset '''
        mock_get_latest_end_date.return_value = None
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_natural_gas_spot_prices_response).encode('utf-8')
        mock_requests_get.return_value = mock_response

        headers = mock_eia_headers
        endpoint = 'natural-gas/pri/fut/data/'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0
        
        response = mock_eia.api_request(endpoint=endpoint,
        headers = headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none,
        offset=offset)
        
        mock_requests_get.assert_called_once_with('https://api.eia.gov/v2/' + endpoint,
            headers={'X-Params': json.dumps({
                'api_key': 'api_key',
                'frequency': 'daily',
                'data': ['value'],
                'facets': {
                    'series': ['RNGWHHD',]
                },
                'sort': [{
                'column': 'period',
                'direction': 'asc'
                }],
                'length': 5000,
                'start': '1999-01-04',
                'offset': offset}),
                'Content-Type': 'application/json'},
            params = {'api_key': 'api_key'},
            timeout=30
        )
        assert response.status_code == 200
        assert response.json() == mock_natural_gas_spot_prices_response

    def test_eia_api_request_error(self, mock_environment_variables, mock_eia, mock_requests_get, mock_eia_headers):
        ''' Test api_request method of EIA class where an error is produced for a given request '''
        mock_requests_get.side_effect = requests.RequestException("API Error")
        headers = mock_eia_headers
        endpoint = 'natural-gas/pri/fut/data/'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0
        
        response = mock_eia.api_request(endpoint=endpoint, 
        headers=headers,
        metadata_folder = metadata_folder,
        metadata_object_key = metadata_object_key,
        metadata_dataset_key = metadata_dataset_key,
        start_date_if_none = start_date_if_none,
        offset=offset)

        assert response[0] == 'Error occurred'
        assert isinstance(response[1], requests.RequestException)
        assert str(response[1] == 'API Error')

    def test_eia_api_get_max_date_with_data_non_monthly(self, mock_eia, mock_natural_gas_spot_prices_response):
        ''' Test get_max_period method of EIA class where data contains daily dates '''
        data = mock_natural_gas_spot_prices_response['response']['data']
        result = mock_eia.get_max_date(data=data, is_monthly=False)
        assert result == '1999-01-05'
    
    def test_eia_api_get_max_date_with_data_monthly(self, mock_eia, mock_natural_gas_monthly_variables_response):
        ''' Test get_max_period method of EIA class where data contains monthly dates '''
        data = mock_natural_gas_monthly_variables_response['response']['data']
        result = mock_eia.get_max_date(data=data, is_monthly=True)
        assert result == '1999-02-01'
    
    def test_eia_api_get_max_date_with_no_data(self, mock_eia):
        ''' Test get_max_period method of EIA class where data is None '''
        data = []
        result = mock_eia.get_max_date(data=data, is_monthly=True)
        assert result is None

    def test_eia_extract_success(self, mock_environment_variables, mock_eia, mock_requests_get, mock_boto3_client, mock_eia_headers, mock_natural_gas_spot_prices_response,
        mock_update_metadata, mock_get_latest_end_date):
        ''' Test eia_extract method of EIA class where max_period is not None '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_natural_gas_spot_prices_response
        mock_response_no_data = MagicMock()
        mock_response_no_data.status_code = 200
        mock_response_no_data.json.return_value = {
        'response': {
            'data': []
        }
        }
        mock_requests_get.side_effect = [mock_response, mock_response_no_data]

        headers = mock_eia_headers
        endpoint = 'natural-gas/pri/fut/data/'
        folder = 'full_program/extraction/'
        object_key = 'natural_gas_spot_prices'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0

        mock_eia.extract(endpoint=endpoint, headers=headers, folder=folder, object_key=object_key,
        metadata_folder=metadata_folder, metadata_object_key=metadata_object_key,
        metadata_dataset_key=metadata_dataset_key, start_date_if_none=start_date_if_none,
        is_monthly=False, offset=offset)
        
        assert mock_requests_get.call_count == 2
        mock_boto3_client.return_value.put_object.assert_called_once()
        mock_update_metadata.assert_called_once_with(
        folder=metadata_folder,
        object_key=metadata_object_key,
        dataset_key=object_key,
        new_date='1999-01-05'
        )
    
    def test_eia_extract_no_data(self, mock_environment_variables, mock_eia, mock_requests_get, mock_boto3_client, mock_eia_headers,
        mock_update_metadata, mock_get_latest_end_date):
        ''' Test eia_extract method of EIA class where max_period is None '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"response": {"data": []}}
        mock_requests_get.return_value = mock_response
        mock_requests_get.side_effect = mock_response
        
        headers = mock_eia_headers
        endpoint = 'natural-gas/pri/fut/data/'
        folder = 'full_program/extraction'
        object_key = 'natural_gas_spot_prices'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'natural_gas_spot_prices'
        start_date_if_none = '1999-01-04'
        offset = 0
        
        mock_eia.extract(endpoint=endpoint, headers=headers, folder=folder, object_key=object_key,
        metadata_folder=metadata_folder, metadata_object_key=metadata_object_key,
        metadata_dataset_key=metadata_dataset_key, start_date_if_none=start_date_if_none,
        is_monthly=False, offset=offset)
        
        assert mock_requests_get.call_count == 1
        mock_boto3_client.put_data.assert_not_called()
        mock_update_metadata.assert_not_called()

class TestNOAA:
    ''' Test class for testing NOAA class '''
    def test_noaa_api_request_success(self, mock_noaa, mock_environment_variables, mock_requests_get, mock_noaa_parameters, mock_noaa_daily_weather_data_response):
        ''' Test api_request method of NOAA class where api request is successful '''
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_noaa_daily_weather_data_response).encode('utf-8')
        mock_requests_get.return_value = mock_response

        parameters = mock_noaa_parameters

        response = mock_noaa.api_request(parameters=parameters)
        
        mock_requests_get.assert_called_once_with(url='https://www.ncei.noaa.gov/cdo-web/api/v2/data',
        headers={'token': 'token'},
        params=parameters,
        timeout=7)
        assert response.status_code == 200
        assert response.json() == mock_noaa_daily_weather_data_response
    
    def test_noaa_api_request_with_timeout_failure(self, mock_noaa, mock_environment_variables, mock_requests_get, mock_noaa_parameters, mock_noaa_daily_weather_data_response):
        ''' Test api_request method of NOAA class where a timeout error is produced for a given request '''
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_noaa_daily_weather_data_response).encode('utf-8')
        mock_requests_get.side_effect = [requests.exceptions.Timeout, requests.exceptions.Timeout, mock_response]
        
        parameters = mock_noaa_parameters
        
        response = mock_noaa.api_request(parameters=parameters)
        
        assert mock_requests_get.call_count == 3
        assert response.status_code == 200
        assert response.json() == mock_noaa_daily_weather_data_response
    
    def test_noaa_api_request_no_timeout_failure(self, mock_noaa, mock_environment_variables, mock_requests_get, mock_noaa_parameters):
        ''' Test api_request method of NOAA class where a timeout error is produced for a given request '''
        mock_requests_get.side_effect = requests.RequestException("API Error")
        
        parameters = mock_noaa_parameters

        response = mock_noaa.api_request(parameters=parameters)
        
        assert response[0] == 'Error occurred'
        assert isinstance(response[1], requests.RequestException)
        assert str(response[1] == 'API Error')
    
    def test_noaa_api_get_max_period_with_data(self, mock_noaa, mock_noaa_daily_weather_data_response):
        ''' Test get_max_period method of NOAA class where data is not None '''
        data = mock_noaa_daily_weather_data_response['results']
        result = mock_noaa.get_max_date(data=data)
        assert result == '2024-05-24'
    
    def test_noaa_api_get_max_period_with_no_data(self, mock_noaa):
        ''' Test get_max_period method of NOAA class where data is None '''
        data = []
        result = mock_noaa.get_max_date(data=data)
        assert result is None

    def test_noaa_extract_success_with_latest_end_date(self, mock_noaa, mock_environment_variables, mock_requests_get, mock_boto3_client, mock_get_latest_end_date, mock_noaa_parameters, mock_noaa_daily_weather_data_response,
    mock_update_metadata):
        ''' Test extract method of NOAA class where latest end date is not None '''
        mock_get_latest_end_date.return_value = datetime(1999, 1, 4, 0, 0, 0).strftime('%Y-%m-%dT%H:%M:%S')
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_noaa_daily_weather_data_response).encode('utf-8')

        no_response = requests.Response()
        no_response.status_code = 200
        no_response._content = json.dumps({'results': []}).encode('utf-8')

        mock_requests_get.side_effect = [mock_response, no_response]

        parameters = mock_noaa_parameters

        folder = 'full_program/extraction'
        object_key = 'daily_weather'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'daily_weather'
        start_date_if_none = ''

        mock_noaa.extract(parameters=parameters, folder=folder, object_key=object_key,metadata_folder=metadata_folder, 
        metadata_object_key=metadata_object_key, metadata_dataset_key=metadata_dataset_key,
        start_date_if_none=start_date_if_none)

        assert mock_requests_get.call_count == 2
        mock_boto3_client.return_value.put_object.assert_called_once()
        mock_update_metadata.assert_called_once_with(
        folder=metadata_folder,
        object_key=metadata_object_key,
        dataset_key=object_key,
        new_date='2024-05-24'
        )
    
    def test_noaa_extract_success_no_latest_end_date(self, mock_noaa, mock_environment_variables, mock_requests_get, mock_boto3_client, mock_get_latest_end_date, mock_noaa_parameters, mock_noaa_daily_weather_data_response,
    mock_update_metadata):
        ''' Test extract method of NOAA class where latest end date is None '''
        mock_get_latest_end_date.return_value = None
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps(mock_noaa_daily_weather_data_response).encode('utf-8')
        
        no_response = requests.Response()
        no_response.status_code = 200
        no_response._content = json.dumps({'results': []}).encode('utf-8')

        mock_requests_get.side_effect = [mock_response, no_response]

        parameters = mock_noaa_parameters

        folder = 'full_program/extraction'
        object_key = 'daily_weather'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'daily_weather'
        start_date_if_none = '1999-01-04'

        mock_noaa.extract(parameters=parameters, folder=folder, object_key=object_key,metadata_folder=metadata_folder, 
        metadata_object_key=metadata_object_key, metadata_dataset_key=metadata_dataset_key,
        start_date_if_none=start_date_if_none)

        assert mock_requests_get.call_count == 2
        mock_boto3_client.return_value.put_object.assert_called_once()
        mock_update_metadata.assert_called_once_with(
        folder=metadata_folder,
        object_key=metadata_object_key,
        dataset_key=object_key,
        new_date='2024-05-24'
        )
    
    def test_noaa_extract_no_data(self, mock_noaa, mock_environment_variables, mock_requests_get, mock_boto3_client, mock_get_latest_end_date, mock_noaa_parameters, mock_update_metadata):
        ''' Test eia_extract method of EIA class where max_period is None '''
        mock_get_latest_end_date.return_value = '1999-01-04'
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = json.dumps({'results': []}).encode('utf-8')
        mock_requests_get.return_value = mock_response

        parameters = mock_noaa_parameters
        
        folder = 'full_program/extraction'
        object_key = 'daily_weather'
        metadata_folder = 'metadata/'
        metadata_object_key = 'metadata'
        metadata_dataset_key = 'daily_weather'
        start_date_if_none = ''

        mock_noaa.extract(parameters=parameters, folder=folder, object_key=object_key,metadata_folder=metadata_folder, 
        metadata_object_key=metadata_object_key, metadata_dataset_key=metadata_dataset_key,
        start_date_if_none=start_date_if_none)
        
        assert mock_requests_get.call_count == 1
        mock_boto3_client.put_data.assert_not_called()
        mock_update_metadata.assert_not_called()


    

    













    









        
    










