# Import modules
import json
import pytest
from unittest.mock import MagicMock
from dags.utils.aws import *
from dags.utils.config import *
from dags.fixtures.fixtures import mock_environment_variables, mock_s3, mock_s3_metadata, mock_boto3_client, mock_get_data, mock_natural_gas_spot_prices_response, mock_metadata_response, mock_sns_notifier

class TestS3:
    ''' Test class for testing S3 class '''
    def test_get_data_valid(self, mock_environment_variables, mock_s3, mock_boto3_client, mock_natural_gas_spot_prices_response):
        ''' Test for get_data method of S3 class for valid folder and object key '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_natural_gas_spot_prices_response).encode('utf-8'))
        }

        result = mock_s3.get_data(s3_key='extraction/natural_gas_spot_prices') # list to get data from the generator

        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key='extraction/natural_gas_spot_prices')
        assert result['response']['data'] == mock_natural_gas_spot_prices_response['response']['data']

    def test_get_data_client_error(self, mock_environment_variables, mock_s3, mock_boto3_client):
        ''' Test for get_data method of S3 class for invalid folder and object key '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.side_effect= Exception('Client Error')

        with pytest.raises(Exception) as excinfo:
            result = mock_s3.get_data(s3_key='invalid_folder/invalid_key')
        
        assert str(excinfo.value) == 'Client Error'

    def test_put_data_valid(self, mock_environment_variables, mock_s3, mock_boto3_client):
        ''' Test for get_data method of S3 class '''
        mock_s3_bucket = mock_boto3_client.return_value

        data = [{'key': 'value'}]
        
        mock_s3.put_data(data, s3_key='extraction/natural_gas_spot_prices')

        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket', Key='extraction/natural_gas_spot_prices',
            Body=json.dumps(data), ContentType='application/json'
        )
    
    def test_put_data_client_error(self, mock_environment_variables, mock_s3, mock_boto3_client):
        ''' Test for put_data method of S3 class for client error '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.put_object.side_effect= Exception('Client Error')

        data = [{'key': 'value'}]

        with pytest.raises(Exception) as excinfo:
            mock_s3.put_data(data, s3_key='extraction/natural_gas_spot_prices')
        
        assert str(excinfo.value) == 'Client Error'

class TestS3Metadata:
    ''' Test class for testing S3Metadata class '''
    def test_get_metadata(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for get_metadata method of S3Metadata class with successful response '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        result = mock_s3_metadata.get_metadata(s3_key='metadata/metadata')

        mock_s3_bucket.get_object.assert_called_once_with(Bucket='bucket', Key='metadata/metadata')
        assert result == mock_metadata_response

    def test_update_metadata_dataset_key_exists_update_key(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key already exists in metadata and existing key for dataset key
        is being updated with a new value '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        s3_key = 'metadata/metadata'
        dataset_key = 'natural_gas_spot_prices'
        latest_end_date = '2026-04-15'

        mock_s3_metadata.update_metadata(s3_key, dataset_key=dataset_key, latest_end_date=latest_end_date)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': {"latest_end_date": "2026-04-15"},
            'natural_gas_rigs_in_operation': {"latest_end_date": "2024-02", "latest_extracted_file_path": "s3_path"},
            'natural_gas_monthly_variables': {"latest_end_date": "2021-02", "latest_transformed_file_path": "s3_path"},
            'daily_weather': {"latest_end_date": "2026-04-13", "latest_transformed_file_path": "s3_path"}
        }
        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket',
            Key='metadata/metadata',
            Body=json.dumps(expected_metadata),
            ContentType= 'application/json'
        )
    
    def test_update_metadata_dataset_key_exists_new_key(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key already exists in metadata and new key for dataset key
        is being updated with a new value '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        s3_key = 'metadata/metadata'
        dataset_key = 'natural_gas_spot_prices'
        latest_transformed_file_path = "s3_transformed_file_path"

        mock_s3_metadata.update_metadata(s3_key, dataset_key=dataset_key, latest_transformed_file_path=latest_transformed_file_path)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': {"latest_end_date": "2026-04-13", "latest_transformed_file_path": "s3_transformed_file_path"},
            'natural_gas_rigs_in_operation': {"latest_end_date": "2024-02", "latest_extracted_file_path": "s3_path"},
            'natural_gas_monthly_variables': {"latest_end_date": "2021-02", "latest_transformed_file_path": "s3_path"},
            'daily_weather': {"latest_end_date": "2026-04-13", "latest_transformed_file_path": "s3_path"}
        }
        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket',
            Key='metadata/metadata',
            Body=json.dumps(expected_metadata),
            ContentType= 'application/json'
        )
    
    def test_update_metadata_dataset_key_not_exist(self, mock_environment_variables, mock_boto3_client, mock_s3_metadata, mock_metadata_response):
        ''' Test for update_metadata method of S3Metadata class where dataset key doesn't exist in metadata '''
        mock_s3_bucket = mock_boto3_client.return_value
        mock_s3_bucket.get_object.return_value = {
            'Body': MagicMock(read=lambda: json.dumps(mock_metadata_response).encode('utf-8'))
        }

        s3_key = 'metadata/metadata'
        dataset_key = 'new_dataset_key'
        latest_end_date = '2026-04-15'

        mock_s3_metadata.update_metadata(s3_key, dataset_key=dataset_key, latest_end_date=latest_end_date)
        
        # Verify that the metadata is updated
        expected_metadata = {
            'natural_gas_spot_prices': {"latest_end_date": "2026-04-13"},
            'natural_gas_rigs_in_operation': {"latest_end_date": "2024-02", "latest_extracted_file_path": "s3_path"},
            'natural_gas_monthly_variables': {"latest_end_date": "2021-02", "latest_transformed_file_path": "s3_path"},
            'daily_weather': {"latest_end_date": "2026-04-13", "latest_transformed_file_path": "s3_path"},
            'new_dataset_key': {"latest_end_date": "2026-04-15"}
        }
        mock_s3_bucket.put_object.assert_called_once_with(
            Bucket='bucket',
            Key='metadata/metadata',
            Body=json.dumps(expected_metadata),
            ContentType= 'application/json'
        )

class DummyTaskInstance:
    ''' Dummy class for simulating task instance for testing SNSNotifier '''
    def __init__(self):
        self.task_id = 'test_task'
        self.log_url = 'http://log_url'

class DummyDAG:
    ''' Dummy class for simulating DAG for testing SNSNotifier '''
    def __init__(self):
        self.dag_id = 'test_dag'

class TestSNSNotifier:
    ''' Test class for testing SNSNotifier class '''
    def test_build_message_full_context(self, mock_sns_notifier):
        ''' Test for build_message method of SNSNotifier class with full context '''

        # Create a context in order to build a message for SNS notification
        context = {
        "task_instance": DummyTaskInstance(),
        "dag": DummyDAG(),
        "execution_date": "2024-01-01",
        "exception": Exception("failure"),
        }

        # Build message and parse to confirm payload
        message = mock_sns_notifier.build_message(context)
        payload = json.loads(message)

        # Assert that the payload is as expected based on context
        assert payload["dag_id"] == "test_dag"
        assert payload["task_id"] == "test_task"
        assert payload["execution_date"] == "2024-01-01"
        assert payload["log_url"] == "http://log_url"
        assert "failure" in payload["exception"]

    def test_build_message_empty_context(self, mock_sns_notifier):
        ''' Test for build_message method of SNSNotifier class with empty context '''
        # Build empty message and confirm payload is empty or has None values as expected
        message = mock_sns_notifier.build_message({})
        payload = json.loads(message)

        assert payload["dag_id"] is None
        assert payload["task_id"] is None
        assert payload["log_url"] is None
        assert payload["exception"] is None
    
    def test_send_calls_publish(self, mock_sns_notifier):
        ''' Test for send method of SNSNotifier class to verify that publish method of SNS client is called with correct parameters '''
        # Create a context in order to send a notification for SNS
        mock_client = mock_sns_notifier.get_client()

        context = {
            "task_instance": DummyTaskInstance(),
            "dag": DummyDAG(),
            "execution_date": "2024-01-01",
        }

        # Call send method to trigger SNS notification
        mock_sns_notifier.send(context)

        # Assert that publish method of SNS client has been called and confirm correct payload
        assert mock_client.publish.called

        _, kwargs = mock_client.publish.call_args

        assert kwargs["TopicArn"] == mock_sns_notifier.topic_arn
        assert kwargs["Subject"] == "Airflow Alert: test_dag.test_task Failed"

        message_payload = json.loads(kwargs["Message"])
        assert message_payload["dag_id"] == "test_dag"


    def test_send_with_missing_context(self, mock_sns_notifier):
        ''' Test for send method of SNSNotifier class to verify that it handles missing context gracefully and still calls publish method of SNS client with correct parameters '''
        # Create an empty context and send notification to SNS
        mock_client = mock_sns_notifier.get_client()

        mock_sns_notifier.send({})

        _, kwargs = mock_client.publish.call_args

        assert kwargs["Subject"] == "Airflow Alert: unknown.unknown Failed"

    def test_call_invokes_send(self, mock_sns_notifier):
        ''' Test for __call__ method of SNSNotifier class to verify that it invokes send method with the provided context '''
        # Create a context and confirm that __call__ method invokes send method with the context
        mock_sns_notifier.send = MagicMock()

        context = {
            "task_instance": DummyTaskInstance(),
            "dag": DummyDAG(),
            "execution_date": "2024-01-01"
        }

        mock_sns_notifier(context)

        mock_sns_notifier.send.assert_called_once_with(context)

        

    





        











