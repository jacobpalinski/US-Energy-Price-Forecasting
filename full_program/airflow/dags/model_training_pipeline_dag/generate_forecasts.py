'''Import relevant modules'''
from datetime import datetime, timedelta
import mlflow
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import BDay, CustomBusinessDay
import numpy as np
from mlflow.tracking import MlflowClient
from sklearn.preprocessing import RobustScaler
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.eia_api_transformation import EiaTransformation
from dags.modelling.mlflow_model import MlflowModel

def generate_forecasts():
    ''' Function that generates forecasts for natural gas prices for 7, 14, 30 and 60 day time horizons 
    using trained model that has been logged in mlflow '''

    # Retrieve current date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Setup mlflow tracking uri and retrieve experiment_id
    mlflow_model = MlflowModel(experiment_name='Natural gas price forecasting production', tracking_uri='http://mlflow:5000')
    mlflow_model.set_tracking_uri()
    experiment_id = mlflow_model.retrieve_experiment_id()

    # Retrieve imputed weather variables
    daily_weather_modelling_imputation_json = s3.get_data(folder='full_program/curated/imputation/', object_key=f'daily_weather_modelling_imputation_base_20241227') # Change to actual date once file has been created
    daily_weather_modelling_imputation_df = EtlTransforms.json_to_df(data=daily_weather_modelling_imputation_json, date_as_index=False)

    # Retrieve training(for fitting scalar) and test data for a given date
    curated_training_data_json = s3.get_data(folder='full_program/curated/training_data/', object_key=f'curated_training_data_{formatted_date}') # Revert back to formatted date once successfully tested
    curated_training_data_df = EtlTransforms.json_to_df(data=curated_training_data_json, date_as_index=True)
    curated_test_data_json = s3.get_data(folder='full_program/curated/test_data/', object_key=f'curated_test_data_{formatted_date}') # Revert back to formatted date once successfully tested
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

    # Retrieve models
    model_7day = mlflow.pyfunc.load_model(model_uri=f"models:/GRU_7_day_horizon_30_{formatted_date}/1")
    model_14day = mlflow.pyfunc.load_model(model_uri=f"models:/GRU_14_day_horizon_21_{formatted_date}/1")
    model_30day = mlflow.pyfunc.load_model(model_uri=f"models:/GRU_30_day_horizon_14_{formatted_date}/1")
    model_60day = mlflow.pyfunc.load_model(model_uri=f"models:/GRU_60_day_horizon_14_{formatted_date}/1")

    # Define the models and forecast horizons
    forecast_horizons = {
    '7day': {'model': model_7day, 'sequence_length': 30, 'number_of_predictions': 7},
    '14day': {'model': model_14day, 'sequence_length': 21, 'number_of_predictions': 14},
    '30day': {'model': model_30day, 'sequence_length': 14, 'number_of_predictions': 30},
    '60day': {'model': model_60day, 'sequence_length': 14, 'number_of_predictions': 60},
    }

    # Generate business days excluding public holidays for extension of test data
    holidays = USFederalHolidayCalendar()

    bday_with_holidays = CustomBusinessDay(holidays=holidays.holidays())

    last_date = curated_test_data_df.index[-1]

    extended_dates = pd.date_range(start=last_date + timedelta(days=1), periods=60, freq=bday_with_holidays)

    for horizon, config in forecast_horizons.items():
        model = config['model']
        sequence_length = config['sequence_length']
        number_of_predictions = config['number_of_predictions']

        # Initialise dataframe and retain last price 
        curated_test_data_df_forecast_horizon_copy = curated_test_data_df.copy()
        
        # Initialise variable to track count of predictions to ensure number < number_of_predictions
        predictions_count = 0

        # Now loop through the rest of the extended_dates
        for date in extended_dates:
            # Break loop if number of predictions required for model have been met
            if predictions_count == number_of_predictions:
                break
            
            #new_row = pd.DataFrame(index=[date], columns=curated_test_data_df_forecast_horizon_copy.columns)
            new_row = pd.DataFrame(
            {col: pd.Series(dtype=curated_test_data_df_forecast_horizon_copy[col].dtype) for col in curated_test_data_df_forecast_horizon_copy.columns},
            index=[date])

            # Calculate 7, 14 and 30 day rolling averages, medians and expotential weighted volatilities
            rolling_average_natural_gas_price_7day = EtlTransforms.calculate_moving_average(curated_test_data_df_forecast_horizon_copy, window=7)
            rolling_average_natural_gas_price_14day = EtlTransforms.calculate_moving_average(curated_test_data_df_forecast_horizon_copy, window=14)
            rolling_average_natural_gas_price_30day = EtlTransforms.calculate_moving_average(curated_test_data_df_forecast_horizon_copy, window=30)
            rolling_median_natural_gas_price_7day = EtlTransforms.calculate_rolling_median(curated_test_data_df_forecast_horizon_copy, window=7)
            rolling_median_natural_gas_price_14day = EtlTransforms.calculate_rolling_median(curated_test_data_df_forecast_horizon_copy, window=14)
            rolling_median_natural_gas_price_30day = EtlTransforms.calculate_rolling_median(curated_test_data_df_forecast_horizon_copy, window=30)
            ew_volatility_natural_gas_price_7day = EtlTransforms.calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=7)
            ew_volatility_natural_gas_price_14day = EtlTransforms.calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=14)
            ew_volatility_natural_gas_price_30day = EtlTransforms.calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=30)
            ew_volatility_natural_gas_price_60day = EtlTransforms.calculate_ew_volatility(curated_test_data_df_forecast_horizon_copy, window=60)
            
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
            new_row.at[date, '60day_ew_volatility price ($/MMBTU)'] = ew_volatility_natural_gas_price_60day

            new_row = EiaTransformation.is_december_or_january(df=new_row)

            new_row['month'] = new_row.index.month
            new_row['week'] = new_row.index.isocalendar().week
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

            # Ensure the merged_df index has same index as new row
            merged_df.index = new_row.index
            
            new_row.at[date, 'min_tavg'] = merged_df.at[date, 'min_tavg']
            new_row.at[date, 'max_tavg'] = merged_df.at[date, 'max_tavg']
            new_row.at[date, 'max_abs_tavg_diff'] = merged_df.at[date, 'max_abs_tavg_diff']
            new_row.at[date, 'max_abs_tavg_diff_relative_to_daily_median'] = merged_df.at[date, 'max_abs_tavg_diff_relative_to_daily_median']
            new_row.at[date, 'hdd_max'] = merged_df.at[date, 'hdd_max']
            new_row.at[date, 'cdd_max'] = merged_df.at[date, 'cdd_max']
            new_row.at[date, 'wci_sum'] = merged_df.at[date, 'wci_sum']
            new_row.at[date, 'snow_sum'] = merged_df.at[date, 'snow_sum']

            # Drop month, week and day from new as they are irrelevant variables for modelling
            new_row = new_row.drop(['month', 'week', 'day'], axis=1)

            # Concatenate new row to existing dataframe
            curated_test_data_df_forecast_horizon_copy = pd.concat([curated_test_data_df_forecast_horizon_copy, new_row])

            # Compute lagged natural gas spot prices for 1day, 2day and 3days
            curated_test_data_df_forecast_horizon_copy.at[date, 'price_1day_lag ($/MMBTU)'] = curated_test_data_df_forecast_horizon_copy.iloc[-2]['price ($/MMBTU)']
            curated_test_data_df_forecast_horizon_copy.at[date, 'price_2day_lag ($/MMBTU)'] = curated_test_data_df_forecast_horizon_copy.iloc[-3]['price ($/MMBTU)']
            curated_test_data_df_forecast_horizon_copy.at[date, 'price_3day_lag ($/MMBTU)'] = curated_test_data_df_forecast_horizon_copy.iloc[-4]['price ($/MMBTU)']

            # Forward fill null values for imports, lng_imports, natural_gas_rigs_in_operation, total_consumption_total_underground_storage_ratio
            curated_test_data_df_forecast_horizon_copy = EtlTransforms.forwardfill_null_values_end_of_series(df=curated_test_data_df_forecast_horizon_copy, columns=['imports', 'lng_imports', 'natural_gas_rigs_in_operation',
            'total_consumption_total_underground_storage_ratio', 'heating_oil_natural_gas_price_ratio'])

            # Normalise test dataframe
            curated_test_data_df_forecast_horizon_copy_X = curated_test_data_df_forecast_horizon_copy.drop('price ($/MMBTU)', axis=1)

            curated_test_data_df_forecast_horizon_copy_Y = curated_test_data_df_forecast_horizon_copy['price ($/MMBTU)']

            curated_test_data_df_forecast_horizon_copy_X[robust_columns] = robust_scaler.transform(curated_test_data_df_forecast_horizon_copy_X[robust_columns])

            curated_test_data_df_forecast_horizon_copy_X['snow_sum'] = np.log(curated_test_data_df_forecast_horizon_copy_X['snow_sum'].astype(float) + 1)
            
            # Create sequences for ML model
            dataset = EtlTransforms.build_dataset(x=curated_test_data_df_forecast_horizon_copy_X, y=curated_test_data_df_forecast_horizon_copy_Y, sequence_length=sequence_length, batch_size=128)

            # Make predictions on each batch and retrieve last predicted value
            last_x_batch = None
            for x_batch, y_batch in dataset:
                last_x_batch = x_batch
            
            # Make predictions on last batch only
            x_batch_np = last_x_batch.numpy()
            predictions = model.predict(x_batch_np)
            last_prediction = predictions[-1, 0]
            
            # Set predicted value as natural gas spot price for a given date
            curated_test_data_df_forecast_horizon_copy.at[date, 'price ($/MMBTU)'] = round(last_prediction, 2)

            predictions_count += 1
        
        # Filter dataframe to only retain the 'price ($/MMBTU)' column
        curated_test_data_df_forecast_horizon_copy_prices_only = curated_test_data_df_forecast_horizon_copy[['price ($/MMBTU)']]

        # Reset index so date column is stored as json
        curated_test_data_df_forecast_horizon_copy_prices_only = curated_test_data_df_forecast_horizon_copy_prices_only.reset_index()

        # Rename index column to date
        curated_test_data_df_forecast_horizon_copy_prices_only = EtlTransforms.rename_columns(df=curated_test_data_df_forecast_horizon_copy_prices_only, renamed_columns={'index': 'date'})

        # Convert date from timestamp to string
        curated_test_data_df_forecast_horizon_copy_prices_only['date'] = curated_test_data_df_forecast_horizon_copy_prices_only['date'].dt.strftime('%Y-%m-%d')

        s3.put_data(data=curated_test_data_df_forecast_horizon_copy_prices_only, folder='full_program/curated/predictions/', object_key=f'predictions_{horizon}')
