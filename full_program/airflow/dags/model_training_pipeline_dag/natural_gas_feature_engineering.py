# Import modules
from datetime import datetime
import pandas as pd
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.transformation.eia_api_transformation import EiaTransformation
from dags.transformation.etl_transforms import EtlTransforms
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def natural_gas_feature_engineering(ts_nodash):
    ''' Function that engineers features from natural gas variables from natural gas datasets '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)

    # Retrieve latest transformed filepaths from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path_natural_gas_spot_prices = metadata.get('natural_gas_spot_prices', {}).get('latest_transformed_file_path')
    latest_transformed_file_path_heating_oil_spot_prices = metadata.get('heating_oil_spot_prices', {}).get('latest_transformed_file_path')
    latest_transformed_file_path_natural_gas_monthly_variables = metadata.get('natural_gas_monthly_variables', {}).get('latest_transformed_file_path')
    latest_transformed_file_path_natural_gas_rigs_in_operation = metadata.get('natural_gas_rigs_in_operation', {}).get('latest_transformed_file_path')

    # Retrieve files from S3 and convert to dataframes
    natural_gas_spot_prices_json = s3.get_data(s3_key=latest_transformed_file_path_natural_gas_spot_prices)
    heating_oil_spot_prices_json = s3.get_data(s3_key=latest_transformed_file_path_heating_oil_spot_prices)
    natural_gas_monthly_variables_json = s3.get_data(s3_key=latest_transformed_file_path_natural_gas_monthly_variables)
    natural_gas_rigs_in_operation_json = s3.get_data(s3_key=latest_transformed_file_path_natural_gas_rigs_in_operation)
    natural_gas_spot_prices_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_json, date_as_index=True)
    heating_oil_spot_prices_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json, date_as_index=True)
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=True)
    natural_gas_rigs_in_operation_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_json, date_as_index=True)

    # Log row counts of various datasets
    logger.info(f"Latest transformed natural gas spot prices dataset contains {len(natural_gas_spot_prices_df)} rows")
    logger.info(f"Latest transformed heating oil spot prices dataset contains {len(heating_oil_spot_prices_df)} rows")
    logger.info(f"Latest transformed natural gas monthly variables dataset contains {len(natural_gas_monthly_variables_df)} rows")
    logger.info(f"Latest transformed natural gas rigs in operation dataset contains {len(natural_gas_rigs_in_operation_df)} rows")

    # Merge dataframes
    curated_training_data_df = EtlTransforms.merge_dataframes(natural_gas_monthly_variables_df=natural_gas_monthly_variables_df, 
    natural_gas_rigs_in_operation_df=natural_gas_rigs_in_operation_df, natural_gas_spot_prices_df=natural_gas_spot_prices_df,
    heating_oil_spot_prices_df=heating_oil_spot_prices_df)

    # Log row count of curated dataset
    logger.info(f"Curated dataset contains {len(curated_training_data_df)} rows")

    # Retrieve latest end date from curated dataset in metadata to determine necessity of feature engineering
    latest_end_date = metadata.get('curated_training_data', {}).get('latest_end_date')
    if latest_end_date is not None:
        latest_end_date = datetime.strptime(latest_end_date, '%Y-%m-%d')
        if curated_training_data_df.index[-1] <= latest_end_date:
            return 'No new data in curated training dataset'

    # Create features from natural gas variables in curated training data
    curated_training_data_df = EiaTransformation.natural_gas_prices_lag(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.heating_oil_to_natural_gas_price_ratio(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.expotential_weighted_natural_gas_price_volatility(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.rolling_average_natural_gas_price(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.rolling_median_natural_gas_price(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.total_consumption_to_total_underground_storage_ratio(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.is_december_or_january(df=curated_training_data_df)

    # Only filter new records for curated training dataset
    if latest_end_date is not None:
        curated_training_data_df = curated_training_data_df[curated_training_data_df.index > latest_end_date]

    # Drop irrelevant columns
    curated_training_data_df = EtlTransforms.drop_columns(df=curated_training_data_df, columns=['commercial_consumption', 'residential_consumption', 
    'total_underground_storage', 'price_heating_oil ($/GAL)'])

    # Log columns after drop_columns transformation
    logger.info(f"Curated training dataset columns after drop_columns transformation: {curated_training_data_df.columns.tolist()}")

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Log successful feature engineering
    logger.info("Features have been successfully engineered for curated training dataset")

    # Put data in S3
    s3.put_data(data=curated_training_data_df, s3_key=f'full_program/curated/training_data/curated_training_data_{ts_nodash}.json')
    s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='curated_training_data', processing_file_path=f'full_program/curated/training_data/curated_training_data_{ts_nodash}.json')

    
    