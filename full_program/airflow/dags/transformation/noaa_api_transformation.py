''' Import modules '''
import numpy as np
import pandas as pd
from datetime import datetime

class NoaaTransformation:
    '''
    Class for performing transformations on data extracted from NOAA API

    Methods
    -------
    modify_date(cls, df):
        Modify date column and creater quarter column for daily weather data
    imputation_df(cls, df):
        Creates dataframe that computes mean values for TMIN, TMAX, AWND, PRCP and SNOW
        for all cities across all quarters
    impute_missing_weather_variables(cls, df, imputation_df):
        Imputes rows which have a missing value for TMIN, TMAX, AWND and SNOW values
    calculate_missing_tavg(cls, df):
        Calculates TAVG (average temperature) for rows where it is missing
    maximum_hdd(cls, df):
        Calculates maximum heating degree day for a given date across all states
    maximum_cdd(cls, df):
        Calculates maximum cooling degree day for a given date across all states
    wci_sum(cls, df):
        Creates aggregated wind chill index (wci) across all states
    snow_sum(cls, df):
        Creates aggregated amount of snow across all states
    min_and_max_average_temperature(cls, df):
        Creates maximum and minimum average temperature for any given state for each date
    max_abs_tavg_diff(cls, df):
        Creates absolute largest day-on-day average temperature movement across all states
    max_abs_tavg_diff_relative_daily_median(cls, df):
        Creates absolute value largest temperature difference relative to daily median across all states
    '''
    @classmethod
    def modify_date(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Modify date column and creater quarter column for daily weather data

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['date'] = pd.to_datetime(df['date'])
        df['quarter'] = df['date'].dt.quarter
        return df
    
    @classmethod
    def imputation_df(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates dataframe that computes mean values for TMIN, TMAX, AWND, PRCP and SNOW
        for all cities across all quarters

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        quarter_months = {
        1: [1, 2, 3],
        2: [4, 5, 6],
        3: [7, 8, 9],
        4: [10, 11, 12]
        }
        output_df = pd.DataFrame(columns=['city', 'quarter', 'datatype', 'impute method', 'impute value'])
        for city in df['city'].unique():
            for quarter, relevant_months in quarter_months.items():
                filtered_df = df[(df['city'] == city) & (df['date'].dt.month.isin(relevant_months))]
                mean_tmin = round(filtered_df[filtered_df['datatype'] == 'TMIN']['value'].mean(), 2)
                mean_tmax = round(filtered_df[filtered_df['datatype'] == 'TMAX']['value'].mean(), 2)
                mean_awnd = round(filtered_df[filtered_df['datatype'] == 'AWND']['value'].mean(), 2)
                median_snow = filtered_df[filtered_df['datatype'] == 'SNOW']['value'].median()
                median_snow = round(median_snow, 2) if not pd.isnull(median_snow) else 0 # Null values where returned for Q1 + Q4 in LA + SF
                new_rows_df = pd.DataFrame([{'city': city, 'quarter': quarter, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': mean_tmin},
                {'city': city, 'quarter': quarter, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': mean_tmax},
                {'city': city, 'quarter': quarter, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': mean_awnd},
                {'city': city, 'quarter': quarter, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': median_snow}])
                output_df = pd.concat([output_df, new_rows_df], ignore_index=True)
        output_df['quarter'] = output_df['quarter'].astype(int)
        return output_df
    
    @classmethod
    def impute_missing_weather_variables(cls, df: pd.DataFrame, imputation_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Imputes rows which have a missing value for TMIN, TMAX, AWND and SNOW values

        Args:
            df (pd.DataFrame): Daily weather data df
            imputation_df (pd.DataFrame): Dataframe that contains imputed values to replace missing values in df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df_group = df.groupby(['city', 'state'])
        quarter_impute_dic = dict(zip(zip(imputation_df['city'], imputation_df['quarter']), imputation_df['impute value']))
        new_rows = []

        for (city, state), group in df_group:
            date = group['date'].iloc[0]
            weather_variables = set(group['datatype'])
            missing_weather_variables = set(['TMIN', 'TMAX', 'AWND', 'SNOW']) - weather_variables
            
            if missing_weather_variables:
                for datatype in missing_weather_variables:
                    imputed_value = quarter_impute_dic.get((city, group['quarter'].iloc[0]))
                    new_row = {'date': date, 'datatype': datatype, 'station': group['station'].iloc[0], 
                    'value': imputed_value, 'city': city, 'state': state,
                    'quarter': group['quarter'].iloc[0]}
                    new_rows.append(new_row)
    
        new_rows_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_rows_df], ignore_index=True)
        return df
    
    @classmethod
    def calculate_missing_tavg(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Calculates TAVG (average temperature) for rows where it is missing

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        tmin_rows = df[df['datatype'] == 'TMIN']
        tmax_rows = df[df['datatype'] == 'TMAX']
        merged_df = pd.merge(tmin_rows, tmax_rows, on=['city', 'date'])
        merged_df['tavg_value'] = round((merged_df['value_x'] + merged_df['value_y']) / 2, 2)
        tavg_rows = df[df['datatype'] == 'TAVG']
        merged_df['combined'] = merged_df['city'] + merged_df['date'].astype(str)
        tavg_rows_combined = tavg_rows['city'] + tavg_rows['date'].astype(str)
        missing_tavg_rows = merged_df[~merged_df['combined'].isin(tavg_rows_combined)]
        new_rows = missing_tavg_rows[['date', 'city']]
        new_rows['datatype'] = 'TAVG'
        new_rows['station'] = missing_tavg_rows['station_x']
        new_rows['value'] = missing_tavg_rows['tavg_value']
        new_rows['state'] = missing_tavg_rows['state_x']
        new_rows['quarter'] = missing_tavg_rows['quarter_x']
        df = pd.concat([df, new_rows], ignore_index=True)
        return df
    
    @classmethod
    def maximum_hdd(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Calculates maximum heating degree day for a given date across all states

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['hdd'] = np.maximum(0, 18.33 - df['tavg'])
        hdd_aggregation_by_state = df.groupby([df.index, 'state'])['hdd'].agg(hdd_max='sum').reset_index()
        hdd_aggregation_max = hdd_aggregation_by_state.groupby('date')['hdd_max'].max()
        df = pd.merge(df, hdd_aggregation_max, left_index=True, right_index=True)
        df = df.drop(columns=['hdd'], axis=1)
        return df
    
    @classmethod
    def maximum_cdd(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Calculates maximum cooling degree day for a given date across all states

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['cdd'] = np.maximum(0, df['tavg'] - 18.33)
        cdd_aggregation_by_state = df.groupby([df.index, 'state'])['cdd'].agg(cdd_max='sum').reset_index()
        cdd_aggregation_max = cdd_aggregation_by_state.groupby('date')['cdd_max'].max()
        df = pd.merge(df, cdd_aggregation_max, left_index=True, right_index=True)
        df = df.drop(columns=['cdd'], axis=1)
        return df
    
    @classmethod
    def wci_sum(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates aggregated wind chill index (wci) across all states

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['wci'] = round(35.74 + (0.6215 * (1.8 * df['tavg'] + 32)) - (35.75 * (2.23694 * df['awnd'])**0.16) + (0.4275 * (1.8 * df['tavg'] + 32) * (2.23694 * df['awnd'])**0.16), 2)
        wci_aggregation = df.groupby(df.index)['wci'].agg(wci_sum='sum')
        df = pd.merge(df, wci_aggregation, left_index=True, right_index=True)
        df = df.drop(columns=['wci'])
        return df
    
    @classmethod
    def snow_sum(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates aggregated amount of snow across all states

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        snow_aggregation = df.groupby(df.index)['snow'].agg(snow_sum='sum')
        df = pd.merge(df, snow_aggregation, left_index=True, right_index=True)
        return df

    @classmethod
    def min_and_max_average_temperature(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates maximum and minimum average temperature for any given state for each date

        Args:
            df (pd.DataFrame): Daily weather data df

        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        avg_tavg_by_state = df.groupby([df.index, 'state'])['tavg'].mean().reset_index()
        min_max_tavg_per_date = avg_tavg_by_state.groupby('date')['tavg'].agg(min_tavg='min', max_tavg='max')
        df = pd.merge(df, min_max_tavg_per_date, left_index=True, right_index=True)
        return df
    
    @classmethod
    def max_abs_tavg_diff(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates absolute largest day-on-day average temperature movement across all states

        Args:
            df (pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        avg_tavg_by_state = df.groupby([df.index, 'state'])['tavg'].mean().reset_index()
        avg_tavg_by_state['tavg_abs_diff'] = avg_tavg_by_state.groupby('state')['tavg'].diff().abs()
        max_abs_tavg_diff_per_date = avg_tavg_by_state.groupby('date')['tavg_abs_diff'].max()
        max_abs_tavg_diff_per_date.name = 'max_abs_tavg_diff'
        df = pd.merge(df, max_abs_tavg_diff_per_date, left_index=True, right_index=True)
        return df
    
    @classmethod
    def max_abs_tavg_diff_relative_to_daily_median(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates absolute value largest temperature difference relative to daily median across all states

        Args:
            df(pd.DataFrame): Daily weather data df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df_copy = df
        df_copy = df_copy.groupby([df_copy.index, 'state'])['tavg'].mean().reset_index()
        df_copy['month'] = df_copy['date'].dt.month
        df_copy['week'] = df_copy['date'].dt.isocalendar().week
        df_copy['day'] = df_copy['date'].dt.day
        daily_median_tavg_by_state = df_copy.groupby(['state', 'month', 'week', 'day'])['tavg'].median().reset_index()
        df_copy = pd.merge(df_copy, daily_median_tavg_by_state, on=['state', 'month', 'week', 'day'], suffixes=('', '_daily_median'), how='left')
        df_copy = df_copy.set_index('date')
        df_copy['tavg_abs_diff_relative_to_daily_median'] = round((df_copy['tavg'] - df_copy['tavg_daily_median']).abs(), 2)
        max_abs_tavg_diff_relative_to_daily_median_df = df_copy.groupby(df_copy.index)['tavg_abs_diff_relative_to_daily_median'].max()
        max_abs_tavg_diff_relative_to_daily_median_df.name = 'max_abs_tavg_diff_relative_to_daily_median'
        df = pd.merge(df, max_abs_tavg_diff_relative_to_daily_median_df, left_index=True, right_index=True)
        return df






        

    









    
