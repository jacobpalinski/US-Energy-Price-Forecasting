# Import modules
from datetime import datetime,timedelta
import pandas as pd
import pandera as pa
from pandera import Column, Check
from datetime import datetime
from dags.utils.config import Config
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def data_quality_checks():
    ''' Function that performs data quality checks on curated training dataset '''
    # Instantiate classes for Config, S3, S3Metadata
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest and previous curated filepaths from metadata in S3 and create dataframes from latest and previous curated filepaths if they exist
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_filepath = metadata.get('curated_training_data', {}).get('latest_file_path')
    latest_curated_training_data_json = s3.get_data(s3_key=latest_filepath)
    latest_curated_training_data_df = EtlTransforms.json_to_df(data=latest_curated_training_data_json, date_as_index=True)

    # Low row count of latest curated training dataset
    logger.info(f"Latest curated training dataset contains {len(latest_curated_training_data_df)} rows")

    previous_filepath = metadata.get('curated_training_data', {}).get('previous_file_path')

    # Check if previous data exists
    if previous_filepath is not None: 
        previous_curated_training_data_json = s3.get_data(s3_key=previous_filepath)
        previous_curated_training_data_df = EtlTransforms.json_to_df(data=previous_curated_training_data_json, date_as_index=True)

        # Low row count of previous curated training dataset
        logger.info(f"Previous curated training dataset contains {len(previous_curated_training_data_df)} rows")

        start_date = previous_curated_training_data_df.index[0]
    
    else:
        start_date = latest_curated_training_data_df.index[0]

    end_date = latest_curated_training_data_df.index[-1]

    # Log start and end dates
    logger.info(f"Start date for data quality checks: {start_date}")
    logger.info(f"End date for data quality checks: {end_date}")

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
    strict=True,
    unique=["date", "price ($/MMBTU)", "imports", "lng_imports", "natural_gas_rigs_in_operation", "price_1day_lag ($/MMBTU)", "price_2day_lag ($/MMBTU)",
            "price_3day_lag ($/MMBTU)", "heating_oil_natural_gas_price_ratio", "7day_ew_volatility price ($/MMBTU)", "14day_ew_volatility price ($/MMBTU)",
            "30day_ew_volatility price ($/MMBTU)", "60day_ew_volatility price ($/MMBTU)", "7day_rolling_average price ($/MMBTU)", "14day_rolling_average price ($/MMBTU)",
            "30day_rolling_average price ($/MMBTU)", "7day_rolling_median price ($/MMBTU)", "14day_rolling_median price ($/MMBTU)", "30day_rolling_median price ($/MMBTU)",
            "total_consumption_total_underground_storage_ratio", "is_dec_or_jan", "hdd_max", "cdd_max", "wci_sum", "snow_sum", "min_tavg", "max_tavg", "max_abs_tavg_diff"])

    # Validate schema
    try:
        schema.validate(latest_curated_training_data_df)
        logger.info("Data quality checks passed")
    except pa.errors.SchemaError:
        logger.exception("Data quality validation failed")