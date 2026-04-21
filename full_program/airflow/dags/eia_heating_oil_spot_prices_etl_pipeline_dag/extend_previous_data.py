''' Import modules '''
from datetime import datetime, timedelta
import pandas as pd
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def extend_previous_data():
    ''' Function that concatenates previous heating oil spot prices transformed dataset with current dataset '''
    # Instantiate classes for Config, S3 and S3Metadata
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve latest extracted timestamp, latest transformed timestamp and previous transformed timestamp
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_extracted_timestamp = metadata.get('heating_oil_spot_prices', {}).get('latest_extracted_timestamp')
    latest_transformed_timestamp = metadata.get('heating_oil_spot_prices', {}).get('latest_transformed_timestamp')
    previous_transformed_timestamp = metadata.get('heating_oil_spot_prices', {}).get('previous_transformed_timestamp')

    # Retrive latest transformed data from S3 and convert to dataframe with date as index
    latest_transformed_file_path = metadata.get('heating_oil_spot_prices', {}).get('latest_transformed_file_path')
    heating_oil_spot_prices_transformed_json = s3.get_data(s3_key=latest_transformed_file_path)
    heating_oil_spot_prices_transformed_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_transformed_json, date_as_index=True)

    # Check if latest extracted timestamp is later than the previous transformed timestamp, provided previous timestamp exists.
    if previous_transformed_timestamp is not None:
        if latest_extracted_timestamp > previous_transformed_timestamp:
            # If latest extracted timestamp is later than previous transformed timestamp concatenate previous transformed dataset with latest transformed dataset
            previous_transformed_file_path = metadata.get('heating_oil_spot_prices', {}).get('previous_transformed_file_path')
            heating_oil_spot_prices_previous_transformed_json = s3.get_data(s3_key=previous_transformed_file_path)
            heating_oil_spot_prices_previous_transformed_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_previous_transformed_json, date_as_index=True)
            heating_oil_spot_prices_df = pd.concat([heating_oil_spot_prices_previous_transformed_df, heating_oil_spot_prices_transformed_df])
        else:
            # If latest extracted timestamp is not later than previous transformed timestamp, retain latest transformed dataset as heating_oil_spot_prices_df
            heating_oil_spot_prices_df = heating_oil_spot_prices_transformed_df
    else:
        # If latest extracted timestamp is not later than previous transformed timestamp, retain latest transformed dataset as heating_oil_spot_prices_df
        heating_oil_spot_prices_df = heating_oil_spot_prices_transformed_df

    # Reset index
    heating_oil_spot_prices_df = heating_oil_spot_prices_df.reset_index()

    # Convert date column to string
    heating_oil_spot_prices_df['date'] = heating_oil_spot_prices_df['date'].dt.strftime('%Y-%m-%d')

    # Update metadata with so that previous and latest transformed file path are the same
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='heating_oil_spot_prices', previous_transformed_file_path=latest_transformed_file_path,
                                previous_transformed_timestamp=latest_transformed_timestamp)

    # Put data in S3
    s3.put_data(data=heating_oil_spot_prices_df, s3_key=latest_transformed_file_path)

