''' Import modules '''
from datetime import datetime, timedelta
import pandas as pd
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def extend_previous_data():
    ''' Function that concatenates previous heating oil spot prices transformed dataset with current dataset '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Previous natural gas spot prices
    previous = datetime.now() - timedelta(days=7)
    formatted_previous_date = previous.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Import previous and current datasets
    heating_oil_spot_prices_json_current = s3.get_data(folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_date}')
    heating_oil_spot_prices_json_previous = s3.get_data(folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_previous_date}')
    heating_oil_spot_prices_df_current = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json_current, date_as_index=True)
    heating_oil_spot_prices_df_previous = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json_previous, date_as_index=True)

    # Combine previous and current datasets
    heating_oil_spot_prices_df = pd.concat([heating_oil_spot_prices_df_previous, heating_oil_spot_prices_df_current])

    # Reset index
    heating_oil_spot_prices_df = heating_oil_spot_prices_df.reset_index()

    # Convert date from timestamp to string
    heating_oil_spot_prices_df['date'] = heating_oil_spot_prices_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=heating_oil_spot_prices_df, folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_date}')

