''' Import modules '''
from datetime import datetime
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.eia_api_transformation import EiaTransformation
from dags.transformation.etl_transforms import EtlTransforms

def natural_gas_feature_engineering():
    ''' Function that engineers features from natural gas variables in curated training data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')
    
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve curated training data from S3 folder
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)

    # Create features from natural gas variables in curated training data
    curated_training_data_df = EiaTransformation.natural_gas_prices_lag(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.heating_oil_to_natural_gas_price_ratio(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.expotential_weighted_natural_gas_price_volatility(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.rolling_average_natural_gas_price(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.rolling_median_natural_gas_price(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.total_consumption_to_total_underground_storage_ratio(df=curated_training_data_df)
    curated_training_data_df = EiaTransformation.is_december_or_january(df=curated_training_data_df)

    # Drop irrelevant columns
    curated_training_data_df = EtlTransforms.drop_columns(df=curated_training_data_df, columns=['commercial_consumption', 'residential_consumption', 
    'total_underground_storage', 'price_heating_oil ($/GAL)'])

    # Reset index so date column is stored as json
    curated_training_data_df = curated_training_data_df.reset_index()

    # Convert date from timestamp to string
    curated_training_data_df['date'] = curated_training_data_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=curated_training_data_df, folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}')

    
    