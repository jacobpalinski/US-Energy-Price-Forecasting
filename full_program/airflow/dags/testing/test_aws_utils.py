''' Import modules '''
import json
import pytest
from unittest.mock import MagicMock
from dags.utils.aws import *
from dags.utils.config import *
from dags.fixtures.fixtures import mock_environment_variables, mock_s3, mock_s3_metadata, mock_boto3_client, mock_get_data, mock_natural_gas_spot_prices_response, mock_metadata_response

class TestS3:
    ''' Test class for testing S3 class '''
    def test_connect(self, mock_environment_variables, mock_s3, mock_boto3_client):
        ''' Test for connection method of S3 class '''
        mock_s3.connect()
        assert mock_s3.s3_client is not None
        mock_boto3_client.assert_called_once_with(
            's3', aws_access_key_id='access-key', aws_secret_access_key='secret-key'
        )

    def test_disconnect(self, mock_environment_variables, mock_s3):
        ''' Test for disconnect method of S3 class '''
        mock_s3.connect()
        mock_s3.s3_client is not None
        mock_s3.disconnect()
        assert not hasattr(mock_s3, 's3_client')

    def test_get_data_valid(self, mock_environment_variables, mock_s3, mock_boto3_client, mock_natural_gas_spot_prices_response):
        ''' Test for get_data method of S3 class for valid folder and object key '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_natural_gas_spot_prices_response).encode('utf-8'))
        }

        folder = 'extraction/'
        object_key = 'natural_gas_spot_prices'
        result = mock_s3.get_data(folder=folder, object_key=object_key) # list to get data from the generator

        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result['response']['data'] == mock_natural_gas_spot_prices_response['response']['data']

    def test_get_data_client_error(self, mock_environment_variables, mock_s3, mock_boto3_client):
        ''' Test for get_data method of S3 class for invalid folder and object key '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.side_effect= Exception('Client Error')

        folder = 'invalid_folder/'
        object_key = 'invalid_key'

        with pytest.raises(Exception) as excinfo:
            result = mock_s3.get_data(folder=folder, object_key=object_key)
        
        assert str(excinfo.value) == 'Client Error'

    def test_put_data(self, mock_environment_variables, mock_s3, mock_boto3_client):
        ''' Test for get_data method of S3 class '''
        mock_s3_bucket = mock_boto3_client.return_value

        data = [{'key': 'value'}]
        folder = 'extraction/'
        object_key = 'natural_gas_spot_prices'
        
        mock_s3.put_data(data, folder, object_key)

        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket', Key=folder + object_key,
            Body=json.dumps(data), ContentType='application/json'
        )

class TestS3Metadata:
    ''' Test class for testing S3Metadata class '''
    def test_get_metadata(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for get_metadata method of S3Metadata class with successful response '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        folder = 'metadata/'
        object_key = 'metadata'

        result = mock_s3_metadata.get_metadata(folder=folder, object_key=object_key)

        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result == mock_metadata_response
    
    def test_get_latest_end_date_single_date(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where a dataset key has only a single end date '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_spot_prices'

        result = mock_s3_metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result == '1999-01-04'
    
    def test_get_latest_end_date_multiple_dates(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where dataset key exists and there are multiple end dates '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'daily_weather'

        result = mock_s3_metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result == '2024-05-23'
    
    def test_get_latest_end_date_no_dates(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where dataset key exists but there are no end dates '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_rigs_in_operation'

        result = mock_s3_metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result is None

    def test_get_latest_end_date_no_dataset_key(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for get_latest_end_date method of S3Metadata class where no dataset key exists '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'invalid_dataset_key'

        result = mock_s3_metadata.get_latest_end_date(folder=folder, object_key=object_key, dataset_key=dataset_key)
        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key=folder + object_key)
        assert result is None

    def test_update_metadata_dataset_key_exists_with_dates(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key already exists in metadata '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        new_date = '2024-06-01'
        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_spot_prices'

        mock_s3_metadata.update_metadata(folder=folder, object_key=object_key, dataset_key=dataset_key, new_date=new_date)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04', '2024-06-01'],
            'natural_gas_rigs_in_operation': [],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21', '2024-05-23']
        }
        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata),
            ContentType= 'application/json'
        )
    
    def test_update_metadata_dataset_key_exists_with_no_dates(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key already exists in metadata '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        new_date = '2024-06'
        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'natural_gas_rigs_in_operation'

        mock_s3_metadata.update_metadata(folder=folder, object_key=object_key, dataset_key=dataset_key, new_date=new_date)

        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04'],
            'natural_gas_rigs_in_operation': ['2024-06'],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21', '2024-05-23']
        }
        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata),
            ContentType= 'application/json'
        )
    
    def test_update_metadata_dataset_key_not_exist(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key doesn't exist in metadata '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        new_date = '2024-06-01'
        folder = 'metadata/'
        object_key = 'metadata'
        dataset_key= 'new_dataset_key'

        mock_s3_metadata.update_metadata(folder=folder, object_key=object_key, dataset_key=dataset_key, new_date=new_date)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': ['1999-01-04'],
            'natural_gas_rigs_in_operation': [],
            'natural_gas_monthly_variables': ['2024-03'],
            'daily_weather': ['2024-05-21', '2024-05-23'],
            'new_dataset_key': ['2024-06-01']
        }
        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket',
            Key='full_program/metadata/metadata',
            Body=json.dumps(expected_metadata),
            ContentType= 'application/json'
        )
        

        

    





        











