''' Import modules '''
import json
import os
import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import RobustScaler
from typing import Callable

class EtlTransforms:
    '''
    Class containing general purpose functions used in ETL process that are not specific 
    to noaa and eia api's

    Methods
    -------
    json_to_df(cls, data):
        Converts json data to a dataframe
    df_to_json(cls, df):
        Converts dataframe to a json file
    drop_columns(cls, df, columns):
        Drops columns from dataframe and returns modified dataframe
    drop_duplicates(cls, df):
        Drops duplicate rows from dataframe and returns modified dataframe
    rename_columns(cls, df):
        Renames columns in dataframe and returns dataframe with new column names
    pivot_columns(cls, df):
        Pivots columns in dataframe and returns modified dataframe
    drop_null(cls, df):
        Drops all null rows in dataframe and returns modified dataframe
    set_date_index(cls, df):
        Sets date as index for given dataframe 
    merge_dataframes(cls, daily_weather_df,  natural_gas_monthly_variables_df, 
    natural_gas_rigs_in_operation_df, natural_gas_spot_prices_df, heating_oil_spot_prices_df):
        Merges dataframes representing each of the transformed sources from transformation folder in S3 Bucket
    forwardfill_null_values_end_of_series(cls, df):
        Forward fills null values for monthly natural gas variables and weather variables at the
        end of time series
    backfill_null_values_start_of_series(cls, df):
        Back fills nulls values at start of series for volatility, lag, rolling
        and maximum day to day average temperature change
    create_test_data(df, holdout):
        Creates test data to be used when evaluating training model performance
    create_sequence(x, y, sequence_length):
        Creates sequences for LSTM
    normalisation(cls, df, fit_transform):
        Normalises dataframe before training machine learning model on dataframe
    '''
    @classmethod
    def json_to_df(cls, data: json, date_as_index: bool) -> pd.DataFrame:
        '''
        Converts json data to a dataframe

        Args:
            data (json): Data in json format to be converted to dataframe
        
        Returns:
            pd.DataFrame: Returns json data as a dataframe
        '''
        df = pd.DataFrame(data)
        if date_as_index is True:
            df = df.set_index('date')
            df.index = pd.to_datetime(df.index)
        return df
    
    @classmethod
    def df_to_json(cls, df: pd.DataFrame) -> json:
        '''
        Converts dataframe to a json file

        Args:
            df (pd.DataFrame): Pandas dataframe to be converted to json format

        Returns:
            json: Data in json format
        '''
        json_data = df.to_json(orient='records')
        return json_data
    
    @classmethod
    def drop_columns(cls, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        '''
        Drops columns from dataframe and returns modified dataframe

        Args:
          df (pd.DataFrame): Pandas dataframe where columns are going to be dropped
          columns (list): List of columns to be removed

        Returns:
            pd.DataFrame: Dataframe with specific columns removed
        '''
        df = df.drop(columns=columns, axis=1)
        return df
    
    @classmethod
    def drop_duplicates(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Drops duplicate rows from dataframe and returns modified dataframe

        Args:
          df (pd.DataFrame): Pandas dataframe where duplicated rows are going to be dropped

        Returns:
            pd.DataFrame: Dataframe with specific columns removed
        '''
        df = df.drop_duplicates(axis=1)
        return df
    
    @classmethod
    def rename_columns(cls, df: pd.DataFrame, renamed_columns: dict) -> pd.DataFrame:
        '''
        Renames columns in a given dataframe

        Args:
            df (pd.DataFrame): Pandas dataframe where columns are going to be renamed
            renamed_columns (dict): Key value pair representing original column name and new column name
        
        Returns:
            pd.DataFrame: Dataframe with renamed columns
        '''
        df = df.rename(columns=renamed_columns)
        return df
    
    @classmethod
    def pivot_columns(cls, df: pd.DataFrame, index: list, column: str, value: str) -> pd.DataFrame:
        ''' 
        Pivots columns in a given dataframe 
        
        Args:
            df (pd.DataFrame): Pandas dataframe where columns are going to be pivoted
            index (list): Columns to index for pivot
            column (str): Column to pivot value for
            value (str): Value displayed for pivoted column
        
        Returns:
            pd.DataFrame: Dataframe with pivoted columns
        '''
        df = df.pivot_table(index=index, columns=column, values=value).reset_index()
        df.columns.name = None
        return df
    
    @classmethod
    def drop_null(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Drops null rows in a given dataframe
        
        Args:
          df (pd.DataFrame): Pandas dataframe where null rows are going to be dropped

        Returns:
            pd.DataFrame: Dataframe with null columns removed 
        '''
        df = df.dropna()
        return df
    
    @classmethod
    def set_date_index(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Sets date as index for given dataframe 
        
        Args:
          df (pd.DataFrame): Pandas dataframe where date is going to be set as index

        Returns:
            pd.DataFrame: Dataframe with date as index
        '''
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        return df
    
    @classmethod
    def merge_dataframes(cls, daily_weather_df: pd.DataFrame,  natural_gas_monthly_variables_df: pd.DataFrame, 
    natural_gas_rigs_in_operation_df: pd.DataFrame, natural_gas_spot_prices_df: pd.DataFrame, heating_oil_spot_prices_df: pd.DataFrame):
        '''
        Merges dataframes representing each of the transformed sources from transformation folder in S3 Bucket

        Args:
            daily_weather_df (pd.DataFrame): Daily weather data df
            natural_gas_monthly_variables_df (pd.DataFrame): Natural gas monthly variables dataframe
            natural_gas_rigs_in_operation_df (pd.DataFrame): Monthly natural gas rigs in operation dataframe
            natural_gas_spot_prices_df (pd.DataFrame): Natural gas spot prices dataframe
            heating_oil_spot_prices_df (pd.DataFrame): Heating oil spot prices dataframe
        
        Returns:
            pd.DataFrame: Merged dataframe
        '''
        common_dates_spot_prices = natural_gas_spot_prices_df.index.intersection(heating_oil_spot_prices_df.index)
        natural_gas_spot_prices_df = natural_gas_spot_prices_df.loc[common_dates_spot_prices]
        heating_oil_spot_prices_df = heating_oil_spot_prices_df.loc[common_dates_spot_prices]
        spot_prices_merged_df = pd.merge(natural_gas_spot_prices_df, heating_oil_spot_prices_df, left_index=True, right_index=True)
        monthly_variables_merged_df = pd.merge(natural_gas_monthly_variables_df, natural_gas_rigs_in_operation_df, left_index=True, right_index=True)
        spot_prices_merged_df = spot_prices_merged_df.reset_index()
        monthly_variables_merged_df = monthly_variables_merged_df.reset_index()
        monthly_variables_merged_df['year_month'] = monthly_variables_merged_df['date'].dt.to_period('M')
        spot_prices_merged_df['year_month'] = spot_prices_merged_df['date'].dt.to_period('M')
        df = pd.merge(spot_prices_merged_df, monthly_variables_merged_df, on='year_month', how='left')
        df = df.drop(columns=['year_month', 'date_y'], axis=1)
        df = df.set_index('date_x')
        df.index.name = 'date'
        df = pd.merge(df, daily_weather_df, left_index=True, right_index=True)
        return df
    
    @classmethod
    def forwardfill_null_values_end_of_series(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Forward fills null values for monthly natural gas variables and weather variables at the
        end of time series

        Args:
            df (pd.DataFrame): Merged dataframe
        
        Returns:
            pd.DataFrame: Returns dataframe with columns with null values end of series forward filled
        '''
        columns_to_ffill = ['imports', 'lng_imports', 'natural_gas_rigs_in_operation', 
        'total_consumption_total_underground_storage_ratio', 'hdd_max', 'cdd_max', 'wci_sum', 'snow_sum', 
        'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median']
        
        for col in columns_to_ffill:
            last_valid_index = df[col].last_valid_index()
            df.loc[last_valid_index:, col] = df.loc[last_valid_index:, col].ffill()
        
        return df

    @classmethod
    def backfill_null_values_start_of_series(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Back fills nulls values at start of series for volatility, lag, rolling
        and maximum day to day average temperature change

        Args:
            df (pd.DataFrame): Merged dataframe
        
        Returns:
            pd.DataFrame: Returns dataframe with columns with null values at start of series back filled
        '''
        columns_to_backfill = ['7day_ew_volatility price ($/MMBTU)', '14day_ew_volatility price ($/MMBTU)', '30day_ew_volatility price ($/MMBTU)',
        '60day_ew_volatility price ($/MMBTU)', 'price_1day_lag ($/MMBTU)', 'price_2day_lag ($/MMBTU)', 'price_3day_lag ($/MMBTU)',
        '7day_rolling_average price ($/MMBTU)', '14day_rolling_average price ($/MMBTU)', '30day_rolling_average price ($/MMBTU)', 
        '7day_rolling_median price ($/MMBTU)', '14day_rolling_median price ($/MMBTU)', '30day_rolling_median price ($/MMBTU)', 'max_abs_tavg_diff']
        df[columns_to_backfill] = df[columns_to_backfill].fillna(method='bfill')
        return df
    
    @classmethod
    def create_test_data(cls, df: pd.DataFrame, holdout: float) -> pd.DataFrame:
        ''' 
        Creates test data to be used when evaluating training model performance

        Args:
            df (pd.DataFrame): Dataframe to be used to create test data
            holdout (float): Percentage of dataframe to be used as test data
            Percentage expressed as value between 0 and 1
        
        Returns:
            pd.DataFrame: Holdout dataframe
          '''
        n_rows = len(df)
        n_holdout_rows = int(n_rows * holdout)
        holdout_df = df.iloc[-n_holdout_rows:]
        return holdout_df

    @classmethod
    def generator(cls, x: pd.DataFrame, y: pd.DataFrame, sequence_length: int) -> None:
        """
        Creates sequences for LSTM

        Args:
            x (pd.DataFrame): Dataframe of input variables into the model
            y (pd.DataFrame): Dataframe of output variables into the model
            sequence_length (int): Number of elements in each sequence

        Returns:
            np.array: Returns array of sequences for both input and output variables
        """
        num_samples = len(y) - sequence_length
        for i in range(num_samples):
            x_seq = x.iloc[i:i + sequence_length]
            y_next = y.iloc[i + sequence_length]
            yield x_seq, y_next

    @classmethod
    def build_dataset(cls, x: pd.DataFrame, y: pd.DataFrame, sequence_length: int, batch_size: int) -> tf.data.Dataset:
        """
        Build dataset from generator
        """
        dataset = tf.data.Dataset.from_generator(
            cls.generator,
            args=(x, y, sequence_length),
            output_signature=(
                tf.TensorSpec(shape=(sequence_length, x.shape[1]), dtype=tf.float64),
                tf.TensorSpec(shape=(), dtype=tf.float64),
            )
        )
        return dataset.batch(batch_size=batch_size).prefetch(tf.data.AUTOTUNE)

    @classmethod
    def normalise(cls, train_df: pd.DataFrame, test_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Normalises dataframe before training machine learning model on dataframe

        Args: 
            df (pd.DataFrame): Merged dataframe
            fit_transform (bool): True or False value indicating whether or not data
            is to be transformed and have a model fit to it

        Returns:
            pd.Dataframe: Returns dataframe with normalised data
        '''
        robust_columns = ['imports', 'lng_imports', 'heating_oil_natural_gas_price_ratio',  '7day_ew_volatility price ($/MMBTU)',
       '14day_ew_volatility price ($/MMBTU)', '30day_ew_volatility price ($/MMBTU)', '60day_ew_volatility price ($/MMBTU)', 
       'price_1day_lag ($/MMBTU)', 'price_2day_lag ($/MMBTU)', 'price_3day_lag ($/MMBTU)',
       '7day_rolling_average price ($/MMBTU)', '14day_rolling_average price ($/MMBTU)',
       '30day_rolling_average price ($/MMBTU)', '7day_rolling_median price ($/MMBTU)','14day_rolling_median price ($/MMBTU)',
       '30day_rolling_median price ($/MMBTU)', 'total_consumption_total_underground_storage_ratio',
       'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median', 
       'hdd_max', 'cdd_max', 'wci_sum', 'natural_gas_rigs_in_operation']
        log_columns = ['snow_sum']
        robust_scaler = RobustScaler()

        # Normalise training data
        train_df[robust_columns] = robust_scaler.fit_transform(train_df[robust_columns])
        
        train_df[log_columns] = np.log(train_df[log_columns] + 1)

        # Normalise test data
        test_df[robust_columns] = robust_scaler.transform(test_df[robust_columns])

        test_df[log_columns] = np.log(test_df[log_columns] + 1)
        
        # Return normalised dataframes
        return train_df, test_df
        
        
        



        



        









        
        
        
        



    


    


