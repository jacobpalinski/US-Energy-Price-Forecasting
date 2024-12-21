'''Import relevant modules'''
import streamlit as st
import mlflow
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import BDay
import numpy as np
from datetime import datetime
from mlflow.tracking import MlflowClient
from sklearn.preprocessing import RobustScaler
from airflow.dags.utils.aws import S3
from airflow.dags.utils.config import Config
from airflow.dags.transformation.etl_transforms import EtlTransforms
from airflow.dags.transformation.eia_api_transformation import EiaTransformation
from airflow.dags.modelling.mlflow_model import MlflowModel

# Instantiate classes for Config, S3
config = Config()
s3 = S3(config=config)

# Setup mlflow tracking uri and retrieve experiment_id
mlflow_model = MlflowModel(experiment_name='Natural gas price forecasting production', tracking_uri='http://mlflow:5000')
mlflow_model.set_tracking_uri()
experiment_id = mlflow_model.retrieve_experiment_id()

# Intialise mlflow client to query the experiment
client = MlflowClient()

# Function to retrieve most recent model
def get_most_recent_model(model_name_prefix: str, experiment_id=experiment_id) -> None:
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        filter_string=f"tags.mlflow.runName LIKE '{model_name_prefix}%'",
        order_by = ['start_time desc'],
        max_results=1
    )

    if runs:
        latest_run = runs[0]
        model_name = latest_run.info.run_name
        run_id = latest_run.info.run_id
        model_uri = f"s3://us-energy-price-forecasting/full_program/artifacts/{run_id}/artifacts/{model_name}/data/model"
    
    else:
        raise ValueError(f"No runs found with model name prefix '{model_name_prefix}' in experiment {experiment_id}")
    
    model = mlflow.pyfunc.load_model(model_uri)
    return model

def calculate_moving_average(df: pd.DataFrame, window: int):
    ''' Calculate moving average price ($/MMBTU) based on a given window '''
    if len(df) >= window:
        return df['price ($/MMBTU)'].iloc[-window:].mean()
    else:
        return None

def calculate_rolling_median(df: pd.DataFrame, window: int):
    ''' Calculate the median price (%/MMBTU) based on a given window '''
    if len(df) >= window:
        return df['price ($/MMBTU)'].iloc[-window:].median()
    else:
        return None

def calculate_ew_volatility(df: pd.DataFrame, window: int):
    ''' Calculate the expotential weighted volatility of price (%/MMBTU) based on a given window '''
    if len(df) >= window:
        return df['price ($/MMBTU)'].ewm(span=window, min_periods=window).std().iloc[-1]
    else:
        return None

# Retrieve imputed weather variables
daily_weather_modelling_imputation_json = s3.get_data(folder='full_program/curated/imputation/', object_key=f'daily_weather_modelling_imputation_base_{formatted_date}') # Change to actual date once file has been created
daily_weather_modelling_imputation_df = EtlTransforms.json_to_df(data=daily_weather_modelling_imputation_json, date_as_index=False)

# Retrieve training(for fitting scalar) and test data for a given date
curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key='curated_training_data_20241118')
curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)
curated_test_data_json = s3.get_data(folder='full_program/curated/test_data/', object_key='curated_test_data_20241118')
curated_test_data_df = EtlTransforms.json_to_df(data=curated_test_data_json, date_as_index=True)

# Create X_train for normalisation purposes
X_train = curated_training_data_df.drop(columns='price ($/MMBTU)')

# Create normalisation scaler for X_train
robust_columns = ['imports', 'lng_imports', 'heating_oil_natural_gas_price_ratio',  '7day_ew_volatility price ($/MMBTU)',
'14day_ew_volatility price ($/MMBTU)', '30day_ew_volatility price ($/MMBTU)', '60day_ew_volatility price ($/MMBTU)', 
'price_1day_lag ($/MMBTU)', 'price_2day_lag ($/MMBTU)', 'price_3day_lag ($/MMBTU)',
'7day_rolling_average price ($/MMBTU)', '14day_rolling_average price ($/MMBTU)',
'30day_rolling_average price ($/MMBTU)', '7day_rolling_median price ($/MMBTU)','14day_rolling_median price ($/MMBTU)',
'30day_rolling_median price ($/MMBTU)', 'total_consumption_total_underground_storage_ratio',
'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median', 
'hdd_max', 'cdd_max', 'wci_sum', 'natural_gas_rigs_in_operation']

robust_scaler = RobustScaler()

# Normalise training data
X_train[robust_columns] = robust_scaler.fit_transform(X_train[robust_columns])

# Get the last date of the original test data
last_row = curated_test_data_df.index[-1]

# Retrieve models
model_7day = get_most_recent_model(model_name_prefix='GRU_7_day_horizon_30', experiment_id=experiment_id)
model_14day = get_most_recent_model(model_name_prefix='GRU_14_day_horizon_21', experiment_id=experiment_id)
model_30day = get_most_recent_model(model_name_prefix='GRU_30_day_horizon_14', experiment_id=experiment_id)
model_60day = get_most_recent_model(model_name_prefix='GRU_60_day_horizon_14', experiment_id=experiment_id)

# Define the models and forecast horizons
forecast_horizons = {
    '7day': {'model': model_7day, 'sequence_length': 30, 'number_of_predictions': 7},
    '14day': {'model': model_14day, 'sequence_length': 21, 'number_of_predictions': 14},
    '30day': {'model': model_30day, 'sequence_length': 14, 'number_of_predictions': 30},
    '60day': {'model': model_60day, 'sequence_length': 14, 'number_of_predictions': 60},
}

# Generate business days excluding public holidays for extension of test data
holidays = USFederalHolidayCalendar()

last_date = curated_test_data_df.index[-1]

extended_dates = pd.date_range(start=last_date, periods=60, freq=BDay(), holidays=holidays.holidays())

new_rows = pd.DataFrame(index=extended_dates, columns=curated_test_data_df.columns)

'''curated_test_data_df = pd.concat([curated_test_data_df, new_rows])'''

'''# Create a normalised dataframe for test data
normalised_test_data_df = curated_test_data_df.copy()

# Normalise columns
normalised_test_data_df[robust_columns] = robust_scaler.transform(normalised_test_data_df[robust_columns])

normalised_test_data_df['snow_sum'] = np.log(normalised_test_data_df['snow_sum'] + 1)'''

# Forward fill null values for imports, lng_imports, natural_gas_rigs_in_operation, total_consumption_total_underground_storage_ratio in curated_test_data_df
'''curated_test_data_df = EtlTransforms.forwardfill_null_values_end_of_series(df=curated_test_data_df, columns=['imports', 'lng_imports', 'natural_gas_rigs_in_operation',
'total_consumption_total_underground_storage_ratio'])'''

for horizon, config in forecast_horizons.items():
    model = config['model']
    sequence_length = config['sequence_length']
    number_of_predictions = config['number_of_predictions']

    # Initialise dataframe and retain last price 
    curated_test_data_df_forecast_horizon_copy = curated_test_data_df.copy()
    '''curated_test_data_df_forecast_horizon_copy.loc[extended_dates[0], 'price ($/MMBTU)'] = curated_test_data_df_forecast_horizon_copy[last_date, 'price ($/MMBTU)']'''
    
    # Initialise variable to track count of predictions to ensure number < number_of_predictions
    predictions_count = 0

    # Now loop through the rest of the extended_dates
    for date in extended_dates:
        # Break loop if number of predictions required for model have been met
        if predictions_count == number_of_predictions:
            break
        
        new_row = pd.DataFrame(index=[date], columns=curated_test_data_df_forecast_horizon_copy.columns)
        '''new_row['price ($/MMBTU)'] = curated_test_data_df_forecast_horizon_copy['price ($/MMBTU)'].iloc[-1]'''

        '''curated_test_data_df_forecast_horizon_copy = pd.concat([curated_test_data_df_forecast_horizon_copy, new_row])'''

        # Calculate 7, 14 and 30 day rolling averages, medians and expotential weighted volatilities
        rolling_average_natural_gas_price_7day = calculate_moving_average(curated_test_data_df_forecast_horizon_copy, window=7)
        rolling_average_natural_gas_price_14day = calculate_moving_average(curated_test_data_df_forecast_horizon_copy, window=14)
        rolling_average_natural_gas_price_30day = calculate_moving_average(curated_test_data_df_forecast_horizon_copy, window=30)
        rolling_median_natural_gas_price_7day = calculate_rolling_median(curated_test_data_df_forecast_horizon_copy, window=7)
        rolling_median_natural_gas_price_14day = calculate_rolling_median(curated_test_data_df_forecast_horizon_copy, window=14)
        rolling_median_natural_gas_price_30day = calculate_rolling_median(curated_test_data_df_forecast_horizon_copy, window=30)
        ew_volatility_natural_gas_price_7day = calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=7)
        ew_volatility_natural_gas_price_14day = calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=14)
        ew_volatility_natural_gas_price_30day = calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=30)
        
        # Update metrics for the new row
        new_row.at[date, '7day_rolling_average price ($/MMBTU)'] = rolling_average_natural_gas_price_7day
        new_row.at[date, '14day_rolling_average price ($/MMBTU)'] = rolling_average_natural_gas_price_14day
        new_row.at[date, '30day_rolling_average price ($/MMBTU)'] = rolling_average_natural_gas_price_30day
        new_row.at[date, '7day_rolling_median price ($/MMBTU)'] = rolling_median_natural_gas_price_7day
        new_row.at[date, '14day_rolling_median price ($/MMBTU)'] = rolling_median_natural_gas_price_14day
        new_row.at[date, '30day_rolling_median price ($/MMBTU)'] = rolling_median_natural_gas_price_30day
        new_row.at[date, '7day_ew_volatility price ($/MMBTU)'] = ew_volatility_natural_gas_price_7day
        new_row.at[date, '14day_ew_volatility price ($/MMBTU)'] = ew_volatility_natural_gas_price_14day
        new_row.at[date, '30day_ew_volatility price ($/MMBTU)'] = ew_volatility_natural_gas_price_30day

        '''# Impute values for is_dec_or_jan and weather variables
        last_row = curated_test_data_df_forecast_horizon_copy.iloc[-1]'''

        new_row = EiaTransformation.is_december_or_january(df=new_row)

        new_row['month'] = new_row.index.month
        new_row['week'] = new_row.index.week
        new_row['day'] = new_row.index.day

        merged_df = pd.merge(new_row, daily_weather_modelling_imputation_df, 
        on=['month', 'week', 'day'], how='left', suffixes=('', '_impute'))
        merged_df['min_tavg'] = merged_df['min_tavg'].fillna(merged_df['min_tavg_impute'])
        merged_df['max_tavg'] = merged_df['max_tavg'].fillna(merged_df['max_tavg_impute'])
        merged_df['max_abs_tavg_diff'] = merged_df['max_abs_tavg_diff'].fillna(merged_df['max_abs_tavg_diff_impute'])
        merged_df['max_abs_tavg_diff_relative_to_daily_median'] = merged_df['max_abs_tavg_diff_relative_to_daily_median'].fillna(merged_df['max_abs_tavg_diff_relative_to_daily_median_impute'])
        merged_df['hdd_max'] = merged_df['hdd_max'].fillna(merged_df['hdd_max_impute'])
        merged_df['cdd_max'] = merged_df['cdd_max'].fillna(merged_df['cdd_max_impute'])
        merged_df['wci_sum'] = merged_df['wci_sum'].fillna(merged_df['wci_sum_impute'])
        merged_df['snow_sum'] = merged_df['snow_sum'].fillna(merged_df['snow_sum_impute'])
        
        new_row.at[date, 'min_tavg'] = merged_df.at[date, 'min_tavg']
        new_row.at[date, 'max_tavg'] = merged_df.at[date, 'max_tavg']
        new_row.at[date, 'max_abs_tavg_diff'] = merged_df.at[date, 'max_abs_tavg_diff']
        new_row.at[date, 'max_abs_tavg_diff_relative_to_daily_median'] = merged_df.at[date, 'max_abs_tavg_diff_relative_to_daily_median']
        new_row.at[date, 'hdd_max'] = merged_df.at[date, 'hdd_max']
        new_row.at[date, 'cdd_max'] = merged_df.at[date, 'cdd_max']
        new_row.at[date, 'wci_sum'] = merged_df.at[date, 'wci_sum']
        new_row.at[date, 'snow_sum'] = merged_df.at[date, 'snow_sum']

        # Concatenate new row to existing dataframe
        curated_test_data_df_forecast_horizon_copy = pd.concat([curated_test_data_df_forecast_horizon_copy, new_row])

        # Forward fill null values for imports, lng_imports, natural_gas_rigs_in_operation, total_consumption_total_underground_storage_ratio
        curated_test_data_df_forecast_horizon_copy = EtlTransforms.forwardfill_null_values_end_of_series(df=curated_test_data_df_forecast_horizon_copy, columns=['imports', 'lng_imports', 'natural_gas_rigs_in_operation',
        'total_consumption_total_underground_storage_ratio'])

        # Normalise test dataframe
        curated_test_data_df_forecast_horizon_copy_X = curated_test_data_df_forecast_horizon_copy.drop('price ($/MMBTU)')

        curated_test_data_df_forecast_horizon_copy_Y = curated_test_data_df_forecast_horizon_copy['price ($/MMBTU)']

        curated_test_data_df_forecast_horizon_copy_X[robust_columns] = robust_scaler.transform(curated_test_data_df_forecast_horizon_copy_X[robust_columns])

        curated_test_data_df_forecast_horizon_copy_X['snow_sum'] = np.log(curated_test_data_df_forecast_horizon_copy_X['snow_sum'] + 1)
        
        # Create sequences for ML model
        dataset = EtlTransforms.build_dataset(x=curated_test_data_df_forecast_horizon_copy_X, y=curated_test_data_df_forecast_horizon_copy_Y, sequence_length=sequence_length, batch_size=128)

        # Make predictions on each batch and retrieve last predicted value
        for x_batch, y_batch in dataset:
            predictions = model.predict(x_batch)
            last_prediction = predictions[-1, 0]
        
        # Set predicted value as natural gas spot price for a given date
        curated_test_data_df_forecast_horizon_copy.at[date, 'price ($/MMBTU)'] = last_prediction

        predictions_count += 1
    
    s3.put_data(data=curated_training_data_df, folder='full_program/curated/predictions/', object_key=f'predictions_{horizon}')


    



        







# Create title
st.title('Hello Streamlit!')