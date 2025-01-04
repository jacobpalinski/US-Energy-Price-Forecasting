''' Import modules '''
import os
import json
import pytest
import requests_mock
import boto3
import pandas as pd
from unittest.mock import patch, MagicMock
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.extraction.eia_api import *
from dags.extraction.noaa_api import *

@pytest.fixture
def mock_environment_variables(mocker):
    ''' 
    Mocks environment variables for testing 
    '''
    mocker.patch.dict(os.environ, {
        'AWS_ACCESS_KEY_ID': 'access-key',
        'AWS_SECRET_ACCESS_KEY': 'secret-key',
        'S3_BUCKET': 'bucket',
        'API_KEY': 'api_key',
        'TOKEN': 'token'
    })

@pytest.fixture
def mock_s3(mocker, mock_environment_variables):
    ''' 
    Mocks S3 class for testing 
    '''
    config = Config()
    s3 = S3(config=config)
    return s3

@pytest.fixture
def mock_s3_metadata(mocker, mock_environment_variables):
    ''' 
    Mocks S3Metadata class for testing 
    '''
    config = Config()
    s3 = S3(config)
    s3_metadata = S3Metadata(config=config)
    return s3_metadata

@pytest.fixture
def mock_boto3_client(mocker):
    ''' 
    Mocks boto3.client for testing 
    '''
    mock_s3_client = mocker.patch('boto3.client', return_value=MagicMock())
    yield mock_s3_client

@pytest.fixture
def mock_get_data(mocker, mock_s3):
    ''' 
    Mocks get_data method of S3 object 
    '''
    mock_s3_get_data = mocker.patch.object(mock_s3, 'get_data')
    return mock_s3_get_data

@pytest.fixture
def mock_eia(mocker, mock_environment_variables):
    ''' 
    Mocks EIA class for testing 
    '''
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)
    eia = EIA(config=config, s3=s3, s3_metadata=s3_metadata)
    return eia

@pytest.fixture
def mock_noaa(mocker, mock_environment_variables):
    ''' 
    Mocks NOAA class for testing 
    '''
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)
    noaa = NOAA(config=config, s3=s3, s3_metadata=s3_metadata)
    return noaa

@pytest.fixture
def mock_requests_get(mocker):
    ''' 
    Mocks requests.get for testing 
    '''
    return mocker.patch('requests.get')

@pytest.fixture
def mock_get_latest_end_date(mocker):
    ''' 
    Mocks get_latest_end_date method of S3Metadata class 
    '''
    mock_s3metadata_get_latest_end_date = mocker.patch.object(S3Metadata, 'get_latest_end_date')
    return mock_s3metadata_get_latest_end_date

@pytest.fixture
def mock_update_metadata(mocker):
    ''' 
    Mocks update_metadata method of S3Metadata class 
    '''
    mock_s3metadata_get_latest_end_date = mocker.patch.object(S3Metadata, 'update_metadata')
    return mock_s3metadata_get_latest_end_date

@pytest.fixture
def mock_eia_headers():
    ''' 
    Mocks headers used in EIA api_request and extract methods 
    '''
    headers = {
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
        'length': 5000
        }
    return headers

@pytest.fixture
def mock_noaa_parameters():
    ''' 
    Mocks parameters used in NOAA api_request and extract methods 
    '''
    parameters = {'datasetid': 'GHCND',
        'datatypeid': ['AWND'],
        'stationid': 'GHCND:USW00094847',
        'enddate': '2024-05-24',
        'units': 'metric',
        'limit': 1000}
    return parameters

@pytest.fixture
def mock_natural_gas_spot_prices_response():
    ''' 
    Mocks data for natural gas spot prices response in EIA API 
    '''
    data = [{"period": "1999-01-04", "duoarea": "RGC", "area-name": "NA", "product": "EPG0", 
            "product-name": "Natural Gas", "process": "PS0", "process-name": "Spot Price", 
            "series": "RNGWHHD", 
            "series-description": "Henry Hub Natural Gas Spot Price (Dollars per Million Btu)",
            "value": "2.1", "units": "$/MMBTU"},
            {"period": "1999-01-05", "duoarea": "RGC", "area-name": "NA", "product": "EPG0",
            "product-name": "Natural Gas", "process": "PS0", "process-name": "Spot Price",
            "series": "RNGWHHD", 
            "series-description": "Henry Hub Natural Gas Spot Price (Dollars per Million Btu)",
            "value": "2.06", "units": "$/MMBTU"}]
    response_dict = {"response": {"data": data}}
    return response_dict

@pytest.fixture
def mock_natural_gas_monthly_variables_response():
    ''' 
    Mocks data for natural gas monthly variables response in EIA API 
    '''
    data = [{"period": "1999-01", "duoarea": "NUS", "area-name": "U.S", "product": "ERTRRG", 
            "product-name": "Rotary Rigs in Operation-Gas", "process": "XRO", "process-name": "Rotary Rigs in Operation", 
            "series": "E_ERTRRG_XR0_NUS_C", 'series-description': 'U.S Natural Gas Rotary Rigs in Operation (Count)',
            "value": "97", "units": "COUNT"},
            {"period": "1999-02", "duoarea": "NUS", "area-name": "U.S", "product": "ERTRRG", 
            "product-name": "Rotary Rigs in Operation-Gas", "process": "XRO", "process-name": "Rotary Rigs in Operation", 
            "series": "E_ERTRRG_XR0_NUS_C", 'series-description': 'U.S Natural Gas Rotary Rigs in Operation (Count)',
            "value": "97", "units": "COUNT"}]
    response_dict = {"response": {"data": data}}
    return response_dict

@pytest.fixture
def mock_noaa_daily_weather_data_response():
    ''' 
    Mocks data for noaa daily weather data response in NOAA API 
    '''
    data = [{'date': datetime(1999, 1, 4, 0, 0, 0).strftime('%Y-%m-%dT%H:%M:%S'), 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city':'Detroit', 'state': 'Michigan'},
            {'date': datetime(1999, 1, 5, 0, 0, 0).strftime('%Y-%m-%dT%H:%M:%S'), 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city':'Detroit', 'state': 'Michigan'},
            {'date': datetime(2024, 5, 24, 0, 0, 0).strftime('%Y-%m-%dT%H:%M:%S'), 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city':'Detroit', 'state': 'Michigan'}]
    response_dict = {"results": data}
    return response_dict

@pytest.fixture
def mock_metadata_response():
    ''' 
    Mocks metadata that is retrieved from S3 bucket to get latest load date for a given dataset 
    '''
    data = {
        'natural_gas_spot_prices': ['1999-01-04'],
        'natural_gas_rigs_in_operation': [],
        'natural_gas_monthly_variables': ['2024-03'],
        'daily_weather': ['2024-05-21', '2024-05-23']
    }
    return data

@pytest.fixture
def df_convert_column_to_float():
    ''' 
    Dataframe used for testing of convert_price_to_float function of EIATransformation class 
    '''
    data = {'period': ['1999-01', '1999-02', '1999-03', '1999-04'],
        'duoarea': ['NUS-Z00', 'NUS-Z00', 'NUS-Z00', 'NUS-Z00'],
        'area-name': ['US.', 'US.', 'US.', 'US.'],
        'product': ['EPG0', 'EPG0', 'EPG0', 'EPG0'],
        'product-name': ['Natural Gas', 'Natural Gas', 'Natural Gas', 'Natural Gas'],
        'process': ['PML', 'PML', 'PML', 'PML'],
        'process-name': ['Imports', 'Imports', 'Imports', 'Imports'],
        'series': ['N9103US3', 'N9103US3', 'N9103US3', 'N9103US3'],
        'series-description': ['Total US Natural Gas Imports', 'Total US Natural Gas Imports', 'Total US Natural Gas Imports', 'Total US Natural Gas Imports'],
        'value': ['3.0', '4.2', '5.1', '6.2'],
        'units': ['$/MCF', '$/MCF', '$/MCF', '$/MCF']}
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_etl_transforms_testing():
    ''' 
    Dataframe to be used for testing of EtlUtils class 
    '''
    data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-06', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': None, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2024-05-24', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_noaa_transformation_testing():
    ''' 
    Dataframe to be used for testing of NoaaTransformation class 
    '''
    data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2000-01-02', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '1999-01-04', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.4, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '1999-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.2, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2000-01-02', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -10, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': -3, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Chicago', 'state': 'Illinois'},
    {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 1, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-04-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -5, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-07-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': -1, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-10-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': -3, 'city': 'New York', 'state': 'New York'},
    {'date': '2001-10-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 3.1, 'city': 'New York', 'state': 'New York'}]
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_noaa_transformation_testing_impute_missing_weather_variables():
    ''' 
    Dataframe to be used for testing of impute_missing_weather_variables method
    of NoaaTransformation class
    '''
    data = [{'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan'},
    {'date': '2001-01-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': None, 'city': 'Detroit', 'state': 'Michigan'}]
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def df_noaa_feature_engineering_testing():
    '''
    Dataframe to be used for testing functions used for feature engineering in NoaaTransformation class
    Will be used to test the following functions:
        maximum_hdd(cls, df)
        maximum_cdd(cls, df)
        wci_sum(cls, df)
        snow_sum(cls, df)
        min_and_max_average_temperature(cls, df)
        max_abs_tavg_diff(cls, df)
        max_abs_tavg_diff_relative_daily_median(cls, df)
    '''
    data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
    '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
    '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
    'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
    'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
    'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
    'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
    'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
    'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
    'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
    'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
    'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def df_eia_feature_engineering_testing():
    '''
    Dataframe to be used for testing functions used for feature engineering in NoaaTransformation class
    Will be used to test the following functions:
        natural_gas_prices_lag(cls, df)
        heating_oil_natural_gas_price_ratio(cls, df)
        expotential_weighted_natural_gas_price_volatility(cls, df)
        rolling_average_natural_gas_price(cls, df)
        rolling_median_natural_gas_price(cls, df)
        total_consumption_to_total_underground_storage_ratio(cls, df)
    '''
    data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
    'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
    'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
    'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
    'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_natural_gas_spot_prices_df_no_missing_date():
    ''' Dataframe containing natural gas spot prices with no missing dates to be used for testing of merge_dataframes function '''
    data = {
    'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_natural_gas_spot_prices_df_missing_date():
    ''' 
    Dataframe containing natural gas spot prices to be used for testing of merge_dataframes function 
    with date 1999-04-01 missing
    '''
    data = {
    'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31'],
    'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_heating_oil_spot_prices_df_no_missing_date():
    ''' 
    Dataframe containing heating oil spot prices with no missing dates to be used for testing of merge_dataframes function
    '''
    data = {
    'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_heating_oil_spot_prices_df_missing_date():
    ''' 
    Dataframe containing heating oil spot prices to be used for testing of merge_dataframes function 
    with date 1999-01-05 missing
    '''
    data = {
    'date': ['1999-01-04', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'price_heating_oil ($/GAL)': [0.346, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_natural_gas_monthly_variables_df_no_missing_date():
    ''' Dataframe containing monthly data for imports, residential consumption, commerical consumption, total_underground_storage with no missing dates
    to be used for testing of merge_dataframes function '''
    data = {
        'date': ['1999-01-01', '1999-02-01', '1999-03-01', '1999-04-01'],
        'imports': [1000, 2000, 3000, 4000],
        'lng_imports': [20, 40, 60, 80],
        'residential_consumption': [2.1, 2.05, 2.04, 1.91],
        'commerical_consumption': [475945.0, 475960.0, 475970.0, 475980.0],
        'total_underground_storage': [6404470.0, 6404480.0, 6404490.0, 6404500.0]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_natural_gas_monthly_variables_df_missing_date():
    ''' Dataframe containing monthly data for imports, residential consumption, commerical consumption, total_underground_storage with date 
    1999-03-01 missing to be used for testing of merge_dataframes function '''
    data = {
        'date': ['1999-01-01', '1999-02-01', '1999-04-01'],
        'imports': [1000, 2000, 4000],
        'lng_imports': [20, 40, 80],
        'residential_consumption': [2.1, 2.05, 1.91],
        'commerical_consumption': [475945.0, 475960.0, 475980.0],
        'total_underground_storage': [6404470.0, 6404480.0, 6404500.0]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date():
    ''' Dataframe containing monthly data for natural gas rigs in operation with no missing dates 
    used for testing of merge_dataframes function
    '''
    data = {
        'date': ['1999-01-01', '1999-02-01', '1999-03-01', '1999-04-01'],
        'natural_gas_rigs_in_operation': [1000, 2000, 3000, 4000]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_natural_gas_rigs_in_operation_df_missing_date():
    ''' Dataframe containing monthly data for natural gas rigs in operation with date 1999-02-01 missing
    used for testing of merge_dataframes function 
    '''
    data = {
        'date': ['1999-01-01', '1999-03-01', '1999-04-01'],
        'natural_gas_rigs_in_operation': [1000, 3000, 4000]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_daily_weather_df_no_missing_date():
    '''
    Dataframe containing daily weather data with no missing dates to be used for testing of merge_dataframes function
    '''
    data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'state': ['Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida'],
    'city': ['Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami'],
    'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7, 
    5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5],
    'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2,
    5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5],
    'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10,
    5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merge_dataframes_daily_weather_df_missing_date():
    '''
    Dataframe containing daily weather data to be used for testing of merge_dataframes function
    with date 1999-03-29 missing
    '''
    data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
    '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
    '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
    '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
    '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
    '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
    '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
    '1999-03-25', '1999-03-26', '1999-03-30', '1999-03-31', '1999-04-01'],
    'state': ['Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida', 'Florida',
    'Florida'],
    'city': ['Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami', 'Miami',
    'Miami'],
    'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7, 
    5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 5, 4, 5],
    'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2,
    5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 5, 4, 5],
    'tavg': [0, 15, 17, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10,
    5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 5, 4, 5]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def df_forwardfill_null_values_end_of_series_with_empty_values():
    ''' 
    Dataframe containing null values end of series used for testing of forwardfill_null_values_end_of_series function 
    '''
    data = {
    'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'imports': [1000, 1000, 1000, None],
    'lng_imports': [20, 40, 60, None],
    'total_consumption_total_underground_storage_ratio': [2.1, 2.05, 2.04, None],
    'natural_gas_rigs_in_operation': [1000, 1000, 2000, None],
    'hdd_max': [10, 5, 1, None],
    'cdd_max': [5, 3, 0, None],
    'wci_sum': [0, 15, 17, None],
    'snow_sum': [0, 2, 2, None],
    'min_tavg': [0, 1, 3, None],
    'max_tavg': [20, 3, 2, None],
    'max_abs_tavg_diff': [1, 3, 2, None],
    'max_abs_tavg_diff_relative_to_daily_median': [4, 3, 2, None]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def df_forwardfill_null_values_end_of_series_no_empty_values():
    '''
    Dataframe containing no null values end of series used for testing of forwardfill_null_values_end_of_series function
    '''
    data = {
    'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
    'imports': [1000, 1000, 1000, 2000],
    'lng_imports': [20, 40, 60, 50],
    'total_consumption_total_underground_storage_ratio': [2.1, 2.05, 2.04, 2.06],
    'natural_gas_rigs_in_operation': [1000, 1000, 2000, 3000],
    'hdd_max': [10, 5, 1, 2],
    'cdd_max': [5, 3, 0, 3],
    'wci_sum': [0, 15, 17, 18],
    'snow_sum': [0, 2, 2, 4],
    'min_tavg': [0, 1, 3, 5],
    'max_tavg': [20, 3, 2, 6],
    'max_abs_tavg_diff': [1, 3, 2, 4],
    'max_abs_tavg_diff_relative_to_daily_median': [4, 3, 2, 6]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def df_backfill_null_values_start_of_series_with_empty_values():
    '''
    Dataframe containing no null values start of series used for testing of backfill_null_values_start_of_series function
    '''
    data = {
        'date': ['1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-06', '1999-03-07', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11',
        '1999-03-12', '1999-03-13', '1999-03-14', '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-20', '1999-03-21', '1999-03-22', '1999-03-23',
        '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-27', '1999-03-28', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01', '1999-04-02', '1999-04-03', '1999-04-04', 
        '1999-04-05', '1999-04-06', '1999-04-07', '1999-04-08', '1999-04-09', '1999-04-10', '1999-04-11', '1999-04-12', '1999-04-13', '1999-04-14', '1999-04-15', '1999-04-16',
        '1999-04-17', '1999-04-18', '1999-04-19', '1999-04-20', '1999-04-21', '1999-04-22', '1999-04-23', '1999-04-24', '1999-04-25', '1999-04-26', '1999-04-27', '1999-04-28', '1999-04-29',
        '1999-04-30', '1999-05-01'],
        'price_1day_lag ($/MMBTU)': [None, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02],
        'price_2day_lag ($/MMBTU)': [None, None, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89],
        'price_3day_lag ($/MMBTU)': [None, None, None, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8],
        '7day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, 0.1, 0.09, 0.09, 0.09, 0.08, 0.07, 0.06, 0.05, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.02, 0.02, 0.03, 0.07, 0.07, 0.07, 0.06, 0.05, 0.05, 0.05, 0.09, 0.09, 0.11, 0.1, 0.09, 0.08, 0.08, 0.07, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.05, 0.11, 0.1],
        '14day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.04, 0.06, 0.07, 0.07, 0.07, 0.07, 0.06, 0.06, 0.08, 0.08, 0.1, 0.1, 0.09, 0.09, 0.08, 0.08, 0.07, 0.07, 0.07, 0.06, 0.06, 0.06, 0.06, 0.05, 0.06, 0.1, 0.1],
        '30day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.05, 0.07, 0.07, 0.08, 0.08, 0.08, 0.08, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.07, 0.07, 0.07, 0.06, 0.07, 0.09, 0.09],
        '60day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 0.07, 0.09, 0.09],
        '7day_rolling_average price ($/MMBTU)': [None, None, None, None, None, None, 1.95, 1.92, 1.88, 1.84, 1.82, 1.81, 1.81, 1.81, 1.79, 1.79, 1.78, 1.78, 1.78, 1.77, 1.76, 1.77, 1.78, 1.79, 1.8, 1.79, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.79, 1.79, 1.77, 1.75, 1.73, 1.71, 1.69, 1.68, 1.67, 1.68, 1.71, 1.74, 1.78, 1.81, 1.83, 1.83, 1.84, 1.82, 1.8, 1.77, 1.75, 1.75, 1.76, 1.77, 1.78, 1.78, 1.81, 1.85, 1.87],
        '14day_rolling_average price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, 1.88, 1.86, 1.83, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.78, 1.79, 1.79, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.78, 1.77, 1.75, 1.74, 1.74, 1.73, 1.72, 1.73, 1.74, 1.75, 1.75, 1.75, 1.75, 1.76, 1.76, 1.77, 1.78, 1.78, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.8, 1.81],
        '30day_rolling_average price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 1.83, 1.82, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.77, 1.77, 1.76, 1.76, 1.76, 1.76, 1.76, 1.77, 1.77, 1.78, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.78, 1.78],
        '7day_rolling_median price ($/MMBTU)': [None, None, None, None, None, None, 1.91, 1.9, 1.87, 1.83, 1.82, 1.81, 1.81, 1.81, 1.78, 1.78, 1.77, 1.76, 1.76, 1.75, 1.75, 1.75, 1.78, 1.79, 1.8, 1.8, 1.8, 1.8, 1.81, 1.81, 1.8, 1.8, 1.79, 1.79, 1.79, 1.79, 1.77, 1.75, 1.73, 1.67, 1.67, 1.67, 1.67, 1.68, 1.72, 1.74, 1.86, 1.86, 1.86, 1.86, 1.81, 1.75, 1.75, 1.75, 1.75, 1.75, 1.75, 1.79, 1.8, 1.8, 1.8, 1.83],
        '14day_rolling_median price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, 1.84, 1.83, 1.82, 1.82, 1.80, 1.80, 1.78, 1.78, 1.78, 1.78, 1.79, 1.80, 1.80, 1.80, 1.79, 1.80, 1.80, 1.80, 1.8, 1.80, 1.80, 1.80, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.74, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.745, 1.75, 1.75, 1.75, 1.75, 1.75, 1.77, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80],
        '30day_rolling_median price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 1.81, 1.81, 1.8, 1.8, 1.80, 1.79, 1.79, 1.79, 1.79, 1.70, 1.79, 1.79, 1.78, 1.78, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76],
        'max_abs_tavg_diff': [None, None, None, 2, 2, 2, 17, 17, 17, 1, 1, 1, 13, 13, 13, 19, 19, 19, 15, 15, 15, 19, 19, 19, 5, 5, 5, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def df_backfill_null_values_start_of_series_no_empty_values():
    '''
    Dataframe containing no null values start of series used for testing of backfill_null_values_start_of_series function
    '''
    data = {
        'date': ['1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-06', '1999-03-07', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11',
        '1999-03-12', '1999-03-13', '1999-03-14', '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-20', '1999-03-21', '1999-03-22', '1999-03-23',
        '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-27', '1999-03-28', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01', '1999-04-02', '1999-04-03', '1999-04-04', 
        '1999-04-05', '1999-04-06', '1999-04-07', '1999-04-08', '1999-04-09', '1999-04-10', '1999-04-11', '1999-04-12', '1999-04-13', '1999-04-14', '1999-04-15', '1999-04-16',
        '1999-04-17', '1999-04-18', '1999-04-19', '1999-04-20', '1999-04-21', '1999-04-22', '1999-04-23', '1999-04-24', '1999-04-25', '1999-04-26', '1999-04-27', '1999-04-28', '1999-04-29',
        '1999-04-30', '1999-05-01'],
        'price_1day_lag ($/MMBTU)': [0, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02],
        'price_2day_lag ($/MMBTU)': [0, 0, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89],
        'price_3day_lag ($/MMBTU)': [0, 0, 0, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8],
        '7day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0.1, 0.09, 0.09, 0.09, 0.08, 0.07, 0.06, 0.05, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.02, 0.02, 0.03, 0.07, 0.07, 0.07, 0.06, 0.05, 0.05, 0.05, 0.09, 0.09, 0.11, 0.1, 0.09, 0.08, 0.08, 0.07, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.05, 0.11, 0.1],
        '14day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.04, 0.06, 0.07, 0.07, 0.07, 0.07, 0.06, 0.06, 0.08, 0.08, 0.1, 0.1, 0.09, 0.09, 0.08, 0.08, 0.07, 0.07, 0.07, 0.06, 0.06, 0.06, 0.06, 0.05, 0.06, 0.1, 0.1],
        '30day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.05, 0.07, 0.07, 0.08, 0.08, 0.08, 0.08, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.07, 0.07, 0.07, 0.06, 0.07, 0.09, 0.09],
        '60day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.07, 0.09, 0.09],
        '7day_rolling_average price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 1.95, 1.92, 1.88, 1.84, 1.82, 1.81, 1.81, 1.81, 1.79, 1.79, 1.78, 1.78, 1.78, 1.77, 1.76, 1.77, 1.78, 1.79, 1.8, 1.79, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.79, 1.79, 1.77, 1.75, 1.73, 1.71, 1.69, 1.68, 1.67, 1.68, 1.71, 1.74, 1.78, 1.81, 1.83, 1.83, 1.84, 1.82, 1.8, 1.77, 1.75, 1.75, 1.76, 1.77, 1.78, 1.78, 1.81, 1.85, 1.87],
        '14day_rolling_average price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.88, 1.86, 1.83, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.78, 1.79, 1.79, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.78, 1.77, 1.75, 1.74, 1.74, 1.73, 1.72, 1.73, 1.74, 1.75, 1.75, 1.75, 1.75, 1.76, 1.76, 1.77, 1.78, 1.78, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.8, 1.81],
        '30day_rolling_average price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.83, 1.82, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.77, 1.77, 1.76, 1.76, 1.76, 1.76, 1.76, 1.77, 1.77, 1.78, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.78, 1.78],
        '7day_rolling_median price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 1.91, 1.9, 1.87, 1.83, 1.82, 1.81, 1.81, 1.81, 1.78, 1.78, 1.77, 1.76, 1.76, 1.75, 1.75, 1.75, 1.78, 1.79, 1.8, 1.8, 1.8, 1.8, 1.81, 1.81, 1.8, 1.8, 1.79, 1.79, 1.79, 1.79, 1.77, 1.75, 1.73, 1.67, 1.67, 1.67, 1.67, 1.68, 1.72, 1.74, 1.86, 1.86, 1.86, 1.86, 1.81, 1.75, 1.75, 1.75, 1.75, 1.75, 1.75, 1.79, 1.8, 1.8, 1.8, 1.83],
        '14day_rolling_median price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.84, 1.83, 1.82, 1.82, 1.80, 1.80, 1.78, 1.78, 1.78, 1.78, 1.79, 1.80, 1.80, 1.80, 1.79, 1.80, 1.80, 1.80, 1.8, 1.80, 1.80, 1.80, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.74, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.745, 1.75, 1.75, 1.75, 1.75, 1.75, 1.77, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80],
        '30day_rolling_median price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.81, 1.81, 1.8, 1.8, 1.80, 1.79, 1.79, 1.79, 1.79, 1.70, 1.79, 1.79, 1.78, 1.78, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76],
        'max_abs_tavg_diff': [0, 0, 0, 2, 2, 2, 17, 17, 17, 1, 1, 1, 13, 13, 13, 19, 19, 19, 15, 15, 15, 19, 19, 19, 5, 5, 5, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def merged_df():
    '''
    Dataframe test data is going to be created from for the create_test_data, create_sequences and normalisation function
    '''
    data = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [1000, 1000, 1000, 2000],
        'lng_imports': [20, 40, 60, 80],
        'heating_oil_natural_gas_price_ratio': [2.1, 2.05, 2.04, 2.06],
        '7day_ew_volatility price ($/MMBTU)': [1.34, 1.34, 1.34, 1.34],
        '14day_ew_volatility price ($/MMBTU)': [2.06, 2.06, 2.06, 2.06],
        '30day_ew_volatility price ($/MMBTU)': [4.2, 4.2, 4.2, 4.2],
        '60day_ew_volatility price ($/MMBTU)': [3.05, 3.05, 3.05, 3.05],
        'price_1day_lag ($/MMBTU)': [2.04, 2.60, 2.45, 1.23],
        'price_2day_lag ($/MMBTU)': [2.60, 2.71, 3.12, 4.90],
        'price_3day_lag ($/MMBTU)': [4.20, 6.19, 2.50, 6.90],
        '7day_rolling_average price ($/MMBTU)': [3.20, 3.20, 3.20, 3.20],
        '14day_rolling_average price ($/MMBTU)': [3.60, 3.60, 3.60, 3.60],
        '30day_rolling_average price ($/MMBTU)': [4.10, 4.10, 4.10, 4.10],
        '7day_rolling_median price ($/MMBTU)': [2.50, 2.50, 2.50, 2.50],
        '14day_rolling_median price ($/MMBTU)': [2.75, 2.75, 2.75, 2.75],
        '30day_rolling_median price ($/MMBTU)': [4.71, 4.71, 4.71, 4.71],
        'total_consumption_total_underground_storage_ratio': [7.05, 7.05, 7.05, 7.05],
        'min_tavg': [-1, -10, -20, 3],
        'max_tavg': [25, 30, 40, 32],
        'max_abs_tavg_diff': [10, 12, 4, 6],
        'max_abs_tavg_diff_relative_to_daily_median': [3, 4, 6, 7],
        'hdd_max': [10, 5, 1, 2],
        'cdd_max': [5, 3, 0, 3],
        'wci_sum': [0, 15, 17, 10],
        'natural_gas_rigs_in_operation': [500, 500, 1000, 2000],
        'snow_sum': [0, 10, 20, 0]
    }
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df

@pytest.fixture
def calculate_rolling_variables_df():
    ''' 
    Dataframe to be used for testing of the calculate_moving_average, calculate_rolling_median and calculate_ew_volatility
    functions used in generate_forecasts DAG task
    '''
    data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08'],
    'price ($/MMBTU)': [1, 2, 3, 4, 5]}
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df


















    









