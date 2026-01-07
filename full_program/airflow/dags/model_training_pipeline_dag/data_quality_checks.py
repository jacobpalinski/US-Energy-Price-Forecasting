''' Import modules '''
from datetime import datetime,timedelta
import pandas as pd
import pandera as pa
from pandera import Column, Check
from datetime import datetime
from dags.utils.config import *
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms

def data_quality_checks():
    ''' Function that performs data quality checks on transformed NOOA weather dataset '''
     # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Previous curated data
    previous = datetime.now() - timedelta(days=7)
    formatted_previous_date = previous.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3 folder
    current_curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    current_curated_training_data_df = EtlTransforms.json_to_df(data=current_curated_training_data_json, date_as_index=True)

    # Retrieve previous training data from S3 folder
    previous_curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_previous_date}')

    # Check if previous data exists
    if previous_curated_training_data_json: 
        previous_curated_training_data_df = EtlTransforms.json_to_df(data=previous_curated_training_data_json, date_as_index=True)
        start_date = previous_curated_training_data_df['date'].iloc[0]
    
    else:
        start_date = current_curated_training_data_df['date'].iloc[0]

    end_date = current_curated_training_data_df['date'].iloc[-1]

    # Pandera schema for data quality checks
    schema = pa.DataFrameSchema(
    columns={
        "date": Column(str, nullable=False, checks=[Check(lambda s: pd.to_datetime(s).ge(start_date).all(), element_wise=False),
        Check(lambda s: pd.to_datetime(s).le(end_date).all(), element_wise=False),]),
        "price ($/MMBTU)": Column(str, nullable=False, checks=Check.ge(0)),
        "imports": Column(float, nullable=False, checks=Check.ge(0)), 
        "lng_imports": Column(int, nullable=False, checks=Check.ge(0)),
        "natural_gas_rigs_in_operation": Column(float, nullable=False, checks=Check.ge(0)),
        "price_1day_lag ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "price_2day_lag ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "price_3day_lag ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "heating_oil_natural_gas_price_ratio": Column(float, nullable=False, checks=Check.ge(0)),
        "7day_ew_volatility price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "14day_ew_volatility price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "30day_ew_volatility price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "60day_ew_volatility price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "7day_rolling_average price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "14day_rolling_average price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "30day_rolling_average price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "7day_rolling_median price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "14day_rolling_median price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "30day_rolling_median price ($/MMBTU)": Column(float, nullable=False, checks=Check.ge(0)),
        "total_consumption_total_underground_storage_ratio": Column(float, nullable=False, checks=Check.ge(0)),
        "is_dec_or_jan": Column(int, nullable=False, checks=Check.isin([0, 1])),
        "hdd_max": Column(float, nullable=False, checks=Check.ge(0)),
        "cdd_max": Column(float, nullable=False, checks=Check.ge(0)),
        "wci_sum": Column(float, nullable=False, checks=Check.ge(0)),
        "snow_sum": Column(float, nullable=False, checks=Check.ge(0)),
        "min_tavg": Column(float, nullable=False),
        "max_tavg": Column(float, nullable=False),
        "max_abs_tavg_diff": Column(float, nullable=False, checks=Check.ge(0)),
    },
    strict=True)

    # Validate schema
    schema.validate(current_curated_training_data_df)