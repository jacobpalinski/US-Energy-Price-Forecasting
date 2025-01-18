''' Import modules '''
import json
import pytest
import pandas as pd
from dags.transformation.etl_transforms import *
from dags.fixtures.fixtures import df_etl_transforms_testing, merge_dataframes_natural_gas_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_spot_prices_df_missing_date, \
merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_heating_oil_spot_prices_df_missing_date, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, \
merge_dataframes_natural_gas_monthly_variables_df_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_missing_date, \
merge_dataframes_daily_weather_df_no_missing_date, merge_dataframes_daily_weather_df_missing_date, df_forwardfill_null_values_end_of_series_with_empty_values,  \
df_forwardfill_null_values_end_of_series_no_empty_values, df_backfill_null_values_start_of_series_with_empty_values, df_backfill_null_values_start_of_series_no_empty_values, \
merged_df, calculate_rolling_variables_df

class TestEtlTransforms:
    ''' Test class for testing EtlUtils class '''
    def test_json_to_df_date_as_index_false(self, df_etl_transforms_testing):
        '''
        Tests json_to_df function of EtlUtils class with date_as_index set to False
        '''
        result_df = EtlTransforms.json_to_df(data=df_etl_transforms_testing, date_as_index=False)
        pd.testing.assert_frame_equal(result_df, df_etl_transforms_testing)
    
    def test_json_to_df_date_as_index_true(self, df_etl_transforms_testing):
        '''
        Tests json_to_df function of EtlUtils class with date_as_index set to True
        '''
        expected_df = df_etl_transforms_testing.set_index('date')
        expected_df.index = pd.to_datetime(expected_df.index)
        result_df = EtlTransforms.json_to_df(data=df_etl_transforms_testing, date_as_index=True)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_df_to_json(self, df_etl_transforms_testing):
        '''
        Tests df_to_json function of EtlUtils class
        '''
        expected_json = df_etl_transforms_testing.to_json(orient='records')
        result_json = EtlTransforms.df_to_json(df=df_etl_transforms_testing)
        assert expected_json == result_json

    def test_drop_columns(self, df_etl_transforms_testing):
        '''
        Tests drop_columns function of EtlUtils class
        '''
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-04', 'datatype': 'AWND', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-05', 'datatype': 'AWND', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-06', 'datatype': 'AWND', 'value': None, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2024-05-24', 'datatype': 'AWND', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlTransforms.drop_columns(df=df_etl_transforms_testing, columns=['station'])
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_rename_columns(self, df_etl_transforms_testing):
        '''
        Tests rename_columns function from EtlUtils class
        '''
        renamed_columns = {'date': 'Date', 'datatype': 'DataType', 'station': 'Station', 'value': 'Value', 'city': 'City', 'state': 'State'}
        data = [{'Date': '1999-01-04', 'DataType': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.3, 'City':'Detroit', 'State': 'Michigan'},
        {'Date': '1999-01-04', 'DataType': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.3, 'City': 'Detroit', 'State': 'Michigan'},
        {'Date': '1999-01-05', 'DataType': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.2, 'City': 'Detroit', 'State': 'Michigan'},
        {'Date': '1999-01-06', 'DataType': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': None, 'City': 'Detroit', 'State': 'Michigan'},
        {'Date': '2024-05-24', 'DataType': 'AWND', 'Station': 'GHCND:USW00094847', 'Value': 4.0, 'City': 'Detroit', 'State': 'Michigan'}]
        expected_df = pd.DataFrame(data)
        result_df = EtlTransforms.rename_columns(df=df_etl_transforms_testing, renamed_columns=renamed_columns)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_pivot_columns(self, df_etl_transforms_testing):
        '''
        Tests pivot_columns function of EtlUtils class
        '''
        index = ['date']
        column = 'datatype'
        value = 'value'
        data = [{'date': '1999-01-04', 'AWND': 4.3},
        {'date': '1999-01-05', 'AWND': 4.2},
        {'date': '2024-05-24', 'AWND': 4.0}]
        expected_df = pd.DataFrame(data)
        result_df = EtlTransforms.pivot_columns(df=df_etl_transforms_testing, index=index, column=column, value=value)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_drop_null(self, df_etl_transforms_testing):
        '''
        Tests drop_null function of EtlUtils class
        '''
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan'},
        {'date': '2024-05-24', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan'}]
        index = [0, 1, 2, 4]
        expected_df = pd.DataFrame(data)
        expected_df.index = index
        result_df = EtlTransforms.drop_null(df=df_etl_transforms_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_date_index(self, df_etl_transforms_testing):
        '''
        Tests set_date_index function of EtlUtils class
        '''
        expected_df = df_etl_transforms_testing.copy()
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        expected_index = pd.to_datetime(['1999-01-04', '1999-01-04', '1999-01-05', '1999-01-06', '2024-05-24'])
        expected_index.name = 'date'
        result_df = EtlTransforms.set_date_index(df=df_etl_transforms_testing)
        result_index = result_df.index
        pd.testing.assert_index_equal(result_index, expected_index)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_merge_dataframe_no_missing_dates_all_datasets(self, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, 
    merge_dataframes_natural_gas_spot_prices_df_no_missing_date, 
    merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date):
        '''
        Tests merge_dataframes function where all datasets to be merged don't contain any missing dates
        '''
        data = {
        'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13', '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', 
        '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26', '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05', '1999-02-08', 
        '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18', '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', 
        '1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12', '1999-03-15', '1999-03-16', '1999-03-17', 
        '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 
        1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 
        1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 
        0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 
        0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'imports': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 4000],
        'lng_imports': [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 60, 60, 
        60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 80],
        'residential_consumption': [2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 
        2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 
        2.04, 2.04, 2.04, 2.04, 2.04, 1.91],
        'commerical_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 
        475945.0, 475945.0, 475945.0, 475945.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 
        475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 
        475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475980.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 
        6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 
        6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 
        6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404500.0],
        'natural_gas_rigs_in_operation': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 3000, 3000, 3000, 3000, 4000]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.merge_dataframes(natural_gas_monthly_variables_df=merge_dataframes_natural_gas_monthly_variables_df_no_missing_date,
        natural_gas_rigs_in_operation_df=merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date,
        natural_gas_spot_prices_df=merge_dataframes_natural_gas_spot_prices_df_no_missing_date,
        heating_oil_spot_prices_df=merge_dataframes_heating_oil_spot_prices_df_no_missing_date)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_merge_dataframe_natural_gas_spot_prices_missing_date(self, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, 
    merge_dataframes_natural_gas_spot_prices_df_missing_date, 
    merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date):
        '''
        Tests merge_dataframes function where natural gas spot prices is missing data for 1999-04-01
        '''
        data = {
        'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13', '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', 
        '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26', '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05', '1999-02-08', 
        '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18', '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', 
        '1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12', '1999-03-15', '1999-03-16', '1999-03-17', 
        '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 
        1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 
        1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 
        0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 
        0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442],
        'imports': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000],
        'lng_imports': [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 60, 60, 
        60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60],
        'residential_consumption': [2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 
        2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 
        2.04, 2.04, 2.04, 2.04, 2.04],
        'commerical_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 
        475945.0, 475945.0, 475945.0, 475945.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 
        475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 
        475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 
        6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 
        6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 
        6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0],
        'natural_gas_rigs_in_operation': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 3000, 3000, 3000, 3000]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.merge_dataframes(natural_gas_monthly_variables_df=merge_dataframes_natural_gas_monthly_variables_df_no_missing_date,
        natural_gas_rigs_in_operation_df=merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date,
        natural_gas_spot_prices_df=merge_dataframes_natural_gas_spot_prices_df_missing_date,
        heating_oil_spot_prices_df=merge_dataframes_heating_oil_spot_prices_df_no_missing_date)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_merge_dataframe_heating_oil_spot_prices_missing_date(self, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, 
    merge_dataframes_natural_gas_spot_prices_df_no_missing_date,
    merge_dataframes_heating_oil_spot_prices_df_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date):
        '''
        Tests merge_dataframes function where heating oil spot prices is missing data for 1999-01-05
        '''
        data = {
        'date': ['1999-01-04', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13', '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', 
        '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26', '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05', '1999-02-08', 
        '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18', '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', 
        '1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12', '1999-03-15', '1999-03-16', '1999-03-17', 
        '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 
        1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 
        1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 
        0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 
        0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'imports': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 4000],
        'lng_imports': [20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 60, 60, 
        60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 80],
        'residential_consumption': [2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 
        2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 
        2.04, 2.04, 2.04, 2.04, 2.04, 1.91],
        'commerical_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 
        475945.0, 475945.0, 475945.0, 475945.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 
        475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 
        475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475980.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 
        6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 
        6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 
        6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404500.0],
        'natural_gas_rigs_in_operation': [1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 2000, 2000, 2000, 2000, 2000, 
        2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 
        3000, 3000, 3000, 3000, 3000, 3000, 3000, 4000]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.merge_dataframes( 
        natural_gas_monthly_variables_df=merge_dataframes_natural_gas_monthly_variables_df_no_missing_date,
        natural_gas_rigs_in_operation_df=merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date,
        natural_gas_spot_prices_df=merge_dataframes_natural_gas_spot_prices_df_no_missing_date,
        heating_oil_spot_prices_df=merge_dataframes_heating_oil_spot_prices_df_missing_date)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_merge_dataframes_natural_gas_monthly_variables_df_missing_date(self, merge_dataframes_natural_gas_monthly_variables_df_missing_date, 
    merge_dataframes_natural_gas_spot_prices_df_no_missing_date, 
    merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date):
        '''
        Tests merge_dataframes function where natural gas monthly variables contains missing data for 1999-03-01
        '''
        data = {
        'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13', '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', 
        '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26', '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05', '1999-02-08', 
        '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18', '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', 
        '1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12', '1999-03-15', '1999-03-16', '1999-03-17', 
        '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 
        1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 
        1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 
        0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 
        0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'imports': [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 2000.0, 
        2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 4000.0],
        'lng_imports': [20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 
        40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 
        None, None, None, None, 80.0],
        'residential_consumption': [2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 
        2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, 2.05, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 
        None, None, None, 1.91],
        'commerical_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 
        475945.0, 475945.0, 475945.0, 475945.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 475960.0, 
        475960.0, 475960.0, 475960.0, 475960.0, 475960.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 
        None, 475980.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 
        6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, 6404480.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 6404500.0],
        'natural_gas_rigs_in_operation': [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 
        1000.0, 1000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, None, None, 
        None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 4000.0]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.merge_dataframes(natural_gas_monthly_variables_df=merge_dataframes_natural_gas_monthly_variables_df_missing_date,
        natural_gas_rigs_in_operation_df=merge_dataframes_natural_gas_rigs_in_operation_df_no_missing_date,
        natural_gas_spot_prices_df=merge_dataframes_natural_gas_spot_prices_df_no_missing_date,
        heating_oil_spot_prices_df=merge_dataframes_heating_oil_spot_prices_df_no_missing_date)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_merge_dataframes_natural_gas_rigs_in_operation_df_missing_date(self, merge_dataframes_natural_gas_monthly_variables_df_no_missing_date, 
    merge_dataframes_natural_gas_spot_prices_df_no_missing_date, 
    merge_dataframes_heating_oil_spot_prices_df_no_missing_date, merge_dataframes_natural_gas_rigs_in_operation_df_missing_date):
        '''
        Tests merge_dataframes function where natural gas monthly variables contains missing data for 1999-02-01
        '''
        data = {
        'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13', '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26', '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05', '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18', '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12', '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'imports': [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 4000.0],
        'lng_imports': [20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 80.0],
        'residential_consumption': [2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 2.04, 1.91],
        'commerical_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475970.0, 475980.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404490.0, 6404500.0],
        'natural_gas_rigs_in_operation': [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 3000.0, 4000.0]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.merge_dataframes(natural_gas_monthly_variables_df=merge_dataframes_natural_gas_monthly_variables_df_no_missing_date,
        natural_gas_rigs_in_operation_df=merge_dataframes_natural_gas_rigs_in_operation_df_missing_date,
        natural_gas_spot_prices_df=merge_dataframes_natural_gas_spot_prices_df_no_missing_date,
        heating_oil_spot_prices_df=merge_dataframes_heating_oil_spot_prices_df_no_missing_date)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_forwardfill_null_values_end_of_series_with_empty_values(self, df_forwardfill_null_values_end_of_series_with_empty_values):
        '''
        Tests forwardfill_null_values_end_of_series function of EtlUtils class where dataframe contains empty values end of series
        '''
        data = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [1000.0, 1000.0, 1000.0, 1000.0],
        'lng_imports': [20.0, 40.0, 60.0, 60.0],
        'total_consumption_total_underground_storage_ratio': [2.1, 2.05, 2.04, 2.04],
        'natural_gas_rigs_in_operation': [1000.0, 1000.0, 2000.0, 2000.0],
        'hdd_max': [10.0, 5.0, 1.0, 1.0],
        'cdd_max': [5.0, 3.0, 0.0, 0.0],
        'wci_sum': [0.0, 15.0, 17.0, 17.0],
        'snow_sum': [0.0, 2.0, 2.0, 2.0],
        'min_tavg': [0.0, 1.0, 3.0, 3.0],
        'max_tavg': [20.0, 3.0, 2.0, 2.0],
        'max_abs_tavg_diff': [1.0, 3.0, 2.0, 2.0],
        'max_abs_tavg_diff_relative_to_daily_median': [4.0, 3.0, 2.0, 2.0]
        }
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.forwardfill_null_values_end_of_series(df=df_forwardfill_null_values_end_of_series_with_empty_values, 
        columns=['imports', 'lng_imports', 'natural_gas_rigs_in_operation', 
        'total_consumption_total_underground_storage_ratio', 'hdd_max', 'cdd_max', 'wci_sum', 'snow_sum', 
        'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median'])
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_forwardfill_null_values_end_of_series_with_no_empty_values(self, df_forwardfill_null_values_end_of_series_no_empty_values):
        '''
        Tests forwardfill_null_values_end_of_series function of EtlUtils class where dataframe contains no empty values end of series
        '''
        data = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [1000, 1000, 1000, 2000],
        'lng_imports': [20, 40, 60, 50],
        'total_consumption_total_underground_storage_ratio': [2.1, 2.05, 2.04, 2.06],
        'natural_gas_rigs_in_operation': [1000, 1000, 2000, 3000],
        'hdd_max': [10, 5, 1, 2],
        'cdd_max': [5, 3, 0, 3],
        'wci_sum': [0, 15, 17, 18],
        'snow_sum': [0, 2, 2, 4],
        'min_tavg': [0, 1, 3, 5],
        'max_tavg': [20, 3, 2, 6],
        'max_abs_tavg_diff': [1, 3, 2, 4],
        'max_abs_tavg_diff_relative_to_daily_median': [4, 3, 2, 6]
        }
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.forwardfill_null_values_end_of_series(df=df_forwardfill_null_values_end_of_series_no_empty_values,
        columns=['imports', 'lng_imports', 'natural_gas_rigs_in_operation', 
        'total_consumption_total_underground_storage_ratio', 'hdd_max', 'cdd_max', 'wci_sum', 'snow_sum', 
        'min_tavg', 'max_tavg', 'max_abs_tavg_diff', 'max_abs_tavg_diff_relative_to_daily_median'])
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_backfill_null_values_start_of_series_with_empty_values(self, df_backfill_null_values_start_of_series_with_empty_values):
        '''
        Tests backfill_null_values_end_of_series function of EtlTransforms class where dataframe contains empty values end of series
        '''
        data = {
        'date': ['1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-06', '1999-03-07', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11',
        '1999-03-12', '1999-03-13', '1999-03-14', '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-20', '1999-03-21', '1999-03-22', '1999-03-23',
        '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-27', '1999-03-28', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01', '1999-04-02', '1999-04-03', '1999-04-04', 
        '1999-04-05', '1999-04-06', '1999-04-07', '1999-04-08', '1999-04-09', '1999-04-10', '1999-04-11', '1999-04-12', '1999-04-13', '1999-04-14', '1999-04-15', '1999-04-16',
        '1999-04-17', '1999-04-18', '1999-04-19', '1999-04-20', '1999-04-21', '1999-04-22', '1999-04-23', '1999-04-24', '1999-04-25', '1999-04-26', '1999-04-27', '1999-04-28', '1999-04-29',
        '1999-04-30', '1999-05-01'],
        'price_1day_lag ($/MMBTU)': [2.1, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02],
        'price_2day_lag ($/MMBTU)': [2.1, 2.1, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89],
        'price_3day_lag ($/MMBTU)': [2.1, 2.1, 2.1, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8],
        '7day_ew_volatility price ($/MMBTU)': [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.09, 0.09, 0.09, 0.08, 0.07, 0.06, 0.05, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.02, 0.02, 0.03, 0.07, 0.07, 0.07, 0.06, 0.05, 0.05, 0.05, 0.09, 0.09, 0.11, 0.1, 0.09, 0.08, 0.08, 0.07, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.05, 0.11, 0.1],
        '14day_ew_volatility price ($/MMBTU)': [0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.04, 0.06, 0.07, 0.07, 0.07, 0.07, 0.06, 0.06, 0.08, 0.08, 0.1, 0.1, 0.09, 0.09, 0.08, 0.08, 0.07, 0.07, 0.07, 0.06, 0.06, 0.06, 0.06, 0.05, 0.06, 0.1, 0.1],
        '30day_ew_volatility price ($/MMBTU)': [0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.05, 0.07, 0.07, 0.08, 0.08, 0.08, 0.08, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.07, 0.07, 0.07, 0.06, 0.07, 0.09, 0.09],
        '60day_ew_volatility price ($/MMBTU)': [0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.09, 0.09],
        '7day_rolling_average price ($/MMBTU)': [1.95, 1.95, 1.95, 1.95, 1.95, 1.95, 1.95, 1.92, 1.88, 1.84, 1.82, 1.81, 1.81, 1.81, 1.79, 1.79, 1.78, 1.78, 1.78, 1.77, 1.76, 1.77, 1.78, 1.79, 1.8, 1.79, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.79, 1.79, 1.77, 1.75, 1.73, 1.71, 1.69, 1.68, 1.67, 1.68, 1.71, 1.74, 1.78, 1.81, 1.83, 1.83, 1.84, 1.82, 1.8, 1.77, 1.75, 1.75, 1.76, 1.77, 1.78, 1.78, 1.81, 1.85, 1.87],
        '14day_rolling_average price ($/MMBTU)': [1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.86, 1.83, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.78, 1.79, 1.79, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.78, 1.77, 1.75, 1.74, 1.74, 1.73, 1.72, 1.73, 1.74, 1.75, 1.75, 1.75, 1.75, 1.76, 1.76, 1.77, 1.78, 1.78, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.8, 1.81],
        '30day_rolling_average price ($/MMBTU)': [1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.83, 1.82, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.77, 1.77, 1.76, 1.76, 1.76, 1.76, 1.76, 1.77, 1.77, 1.78, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.78, 1.78],
        '7day_rolling_median price ($/MMBTU)': [1.91, 1.91, 1.91, 1.91, 1.91, 1.91, 1.91, 1.9, 1.87, 1.83, 1.82, 1.81, 1.81, 1.81, 1.78, 1.78, 1.77, 1.76, 1.76, 1.75, 1.75, 1.75, 1.78, 1.79, 1.8, 1.8, 1.8, 1.8, 1.81, 1.81, 1.8, 1.8, 1.79, 1.79, 1.79, 1.79, 1.77, 1.75, 1.73, 1.67, 1.67, 1.67, 1.67, 1.68, 1.72, 1.74, 1.86, 1.86, 1.86, 1.86, 1.81, 1.75, 1.75, 1.75, 1.75, 1.75, 1.75, 1.79, 1.8, 1.8, 1.8, 1.83],
        '14day_rolling_median price ($/MMBTU)': [1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.84, 1.83, 1.82, 1.82, 1.80, 1.80, 1.78, 1.78, 1.78, 1.78, 1.79, 1.80, 1.80, 1.80, 1.79, 1.80, 1.80, 1.80, 1.8, 1.80, 1.80, 1.80, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.74, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.745, 1.75, 1.75, 1.75, 1.75, 1.75, 1.77, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80],
        '30day_rolling_median price ($/MMBTU)': [1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.81, 1.8, 1.8, 1.80, 1.79, 1.79, 1.79, 1.79, 1.70, 1.79, 1.79, 1.78, 1.78, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76],
        'max_abs_tavg_diff': [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 17, 17, 17, 1, 1, 1, 13, 13, 13, 19, 19, 19, 15, 15, 15, 19, 19, 19, 5, 5, 5, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
        }
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.backfill_null_values_start_of_series(df_backfill_null_values_start_of_series_with_empty_values)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_backfill_null_values_start_of_series_no_empty_values(self, df_backfill_null_values_start_of_series_no_empty_values):
        '''
        Tests backfill_null_values_end_of_series function of EtlTransforms class where dataframe contains no empty values end of series
        '''
        data = {
        'date': ['1999-03-01', '1999-03-02', '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-06', '1999-03-07', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11',
        '1999-03-12', '1999-03-13', '1999-03-14', '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-20', '1999-03-21', '1999-03-22', '1999-03-23',
        '1999-03-24', '1999-03-25', '1999-03-26', '1999-03-27', '1999-03-28', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01', '1999-04-02', '1999-04-03', '1999-04-04', 
        '1999-04-05', '1999-04-06', '1999-04-07', '1999-04-08', '1999-04-09', '1999-04-10', '1999-04-11', '1999-04-12', '1999-04-13', '1999-04-14', '1999-04-15', '1999-04-16',
        '1999-04-17', '1999-04-18', '1999-04-19', '1999-04-20', '1999-04-21', '1999-04-22', '1999-04-23', '1999-04-24', '1999-04-25', '1999-04-26', '1999-04-27', '1999-04-28', '1999-04-29',
        '1999-04-30', '1999-05-01'],
        'price_1day_lag ($/MMBTU)': [0, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02],
        'price_2day_lag ($/MMBTU)': [0, 0, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89],
        'price_3day_lag ($/MMBTU)': [0, 0, 0, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8],
        '7day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0.1, 0.09, 0.09, 0.09, 0.08, 0.07, 0.06, 0.05, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.02, 0.02, 0.03, 0.07, 0.07, 0.07, 0.06, 0.05, 0.05, 0.05, 0.09, 0.09, 0.11, 0.1, 0.09, 0.08, 0.08, 0.07, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.05, 0.11, 0.1],
        '14day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.04, 0.06, 0.07, 0.07, 0.07, 0.07, 0.06, 0.06, 0.08, 0.08, 0.1, 0.1, 0.09, 0.09, 0.08, 0.08, 0.07, 0.07, 0.07, 0.06, 0.06, 0.06, 0.06, 0.05, 0.06, 0.1, 0.1],
        '30day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.05, 0.07, 0.07, 0.08, 0.08, 0.08, 0.08, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.07, 0.07, 0.07, 0.06, 0.07, 0.09, 0.09],
        '60day_ew_volatility price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.07, 0.09, 0.09],
        '7day_rolling_average price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 1.95, 1.92, 1.88, 1.84, 1.82, 1.81, 1.81, 1.81, 1.79, 1.79, 1.78, 1.78, 1.78, 1.77, 1.76, 1.77, 1.78, 1.79, 1.8, 1.79, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.79, 1.79, 1.77, 1.75, 1.73, 1.71, 1.69, 1.68, 1.67, 1.68, 1.71, 1.74, 1.78, 1.81, 1.83, 1.83, 1.84, 1.82, 1.8, 1.77, 1.75, 1.75, 1.76, 1.77, 1.78, 1.78, 1.81, 1.85, 1.87],
        '14day_rolling_average price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.88, 1.86, 1.83, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.78, 1.79, 1.79, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.78, 1.77, 1.75, 1.74, 1.74, 1.73, 1.72, 1.73, 1.74, 1.75, 1.75, 1.75, 1.75, 1.76, 1.76, 1.77, 1.78, 1.78, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.8, 1.81],
        '30day_rolling_average price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.83, 1.82, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.77, 1.77, 1.76, 1.76, 1.76, 1.76, 1.76, 1.77, 1.77, 1.78, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.78, 1.78],
        '7day_rolling_median price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 1.91, 1.9, 1.87, 1.83, 1.82, 1.81, 1.81, 1.81, 1.78, 1.78, 1.77, 1.76, 1.76, 1.75, 1.75, 1.75, 1.78, 1.79, 1.8, 1.8, 1.8, 1.8, 1.81, 1.81, 1.8, 1.8, 1.79, 1.79, 1.79, 1.79, 1.77, 1.75, 1.73, 1.67, 1.67, 1.67, 1.67, 1.68, 1.72, 1.74, 1.86, 1.86, 1.86, 1.86, 1.81, 1.75, 1.75, 1.75, 1.75, 1.75, 1.75, 1.79, 1.8, 1.8, 1.8, 1.83],
        '14day_rolling_median price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.84, 1.83, 1.82, 1.82, 1.80, 1.80, 1.78, 1.78, 1.78, 1.78, 1.79, 1.80, 1.80, 1.80, 1.79, 1.80, 1.80, 1.80, 1.8, 1.80, 1.80, 1.80, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.74, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.745, 1.75, 1.75, 1.75, 1.75, 1.75, 1.77, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80],
        '30day_rolling_median price ($/MMBTU)': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.81, 1.81, 1.8, 1.8, 1.80, 1.79, 1.79, 1.79, 1.79, 1.70, 1.79, 1.79, 1.78, 1.78, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.79, 1.78, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76],
        'max_abs_tavg_diff': [0, 0, 0, 2, 2, 2, 17, 17, 17, 1, 1, 1, 13, 13, 13, 19, 19, 19, 15, 15, 15, 19, 19, 19, 5, 5, 5, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
        }
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.backfill_null_values_start_of_series(df=df_backfill_null_values_start_of_series_no_empty_values)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_create_test_data(self, merged_df):
        ''' Tests create_test_data function of EtlTransforms class '''
        data = {
        'date': ['1999-03-31', '1999-04-01'],
        'imports': [1000, 2000],
        'lng_imports': [60, 80],
        'heating_oil_natural_gas_price_ratio': [2.04, 2.06],
        '7day_ew_volatility price ($/MMBTU)': [1.34, 1.34],
        '14day_ew_volatility price ($/MMBTU)': [2.06, 2.06],
        '30day_ew_volatility price ($/MMBTU)': [4.2, 4.2],
        '60day_ew_volatility price ($/MMBTU)': [3.05, 3.05],
        'price_1day_lag ($/MMBTU)': [2.45, 1.23],
        'price_2day_lag ($/MMBTU)': [3.12, 4.90],
        'price_3day_lag ($/MMBTU)': [2.50, 6.90],
        '7day_rolling_average price ($/MMBTU)': [3.20, 3.20],
        '14day_rolling_average price ($/MMBTU)': [3.60, 3.60],
        '30day_rolling_average price ($/MMBTU)': [4.10, 4.10],
        '7day_rolling_median price ($/MMBTU)': [2.50, 2.50],
        '14day_rolling_median price ($/MMBTU)': [2.75, 2.75],
        '30day_rolling_median price ($/MMBTU)': [4.71, 4.71],
        'total_consumption_total_underground_storage_ratio': [7.05, 7.05],
        'min_tavg': [-20, 3],
        'max_tavg': [40, 32],
        'max_abs_tavg_diff': [4, 6],
        'max_abs_tavg_diff_relative_to_daily_median': [6, 7],
        'hdd_max': [1, 2],
        'cdd_max': [0, 3],
        'wci_sum': [17, 10],
        'natural_gas_rigs_in_operation': [1000, 2000],
        'snow_sum': [20, 0]
        }
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EtlTransforms.create_test_data(df=merged_df, holdout=0.5)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_generator(self):
        ''' Tests generator function of EtlTransforms class '''
        x_train = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [1000, 1000, 1000, 2000],
        'lng_imports': [20, 40, 60, 80],
        'heating_oil_natural_gas_price_ratio': [2.1, 2.05, 2.04, 2.06],
        '7day_ew_volatility price ($/MMBTU)': [1.34, 1.34, 1.34, 1.34],
        '14day_ew_volatility price ($/MMBTU)': [2.06, 2.06, 2.06, 2.06],
        '30day_ew_volatility price ($/MMBTU)': [4.2, 4.2, 4.2, 4.2],
        '60day_ew_volatility price ($/MMBTU)': [3.05, 3.05, 3.05, 3.05],
        'price_1day_lag ($/MMBTU)': [2.04, 2.60, 2.45, 1.23],
        'price_2day_lag ($/MMBTU)': [2.60, 2.71, 3.12, 4.90],
        'price_3day_lag ($/MMBTU)': [4.20, 6.19, 2.50, 6.90],
        '7day_rolling_average price ($/MMBTU)': [3.20, 3.20, 3.20, 3.20],
        '14day_rolling_average price ($/MMBTU)': [3.60, 3.60, 3.60, 3.60],
        '30day_rolling_average price ($/MMBTU)': [4.10, 4.10, 4.10, 4.10],
        '7day_rolling_median price ($/MMBTU)': [2.50, 2.50, 2.50, 2.50],
        '14day_rolling_median price ($/MMBTU)': [2.75, 2.75, 2.75, 2.75],
        '30day_rolling_median price ($/MMBTU)': [4.71, 4.71, 4.71, 4.71],
        'total_consumption_total_underground_storage_ratio': [7.05, 7.05, 7.05, 7.05],
        'min_tavg': [-1, -10, -20, 3],
        'max_tavg': [25, 30, 40, 32],
        'max_abs_tavg_diff': [10, 12, 4, 6],
        'max_abs_tavg_diff_relative_to_daily_median': [3, 4, 6, 7],
        'hdd_max': [10, 5, 1, 2],
        'cdd_max': [5, 3, 0, 3],
        'wci_sum': [0, 15, 17, 10],
        'natural_gas_rigs_in_operation': [500, 500, 1000, 2000],
        'snow_sum': [0, 10, 20, 0]
        }
        y_train = {'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.10, 2.08]}
        x_train_df = pd.DataFrame(x_train)
        x_train_df['date'] = pd.to_datetime(x_train_df['date'])
        x_train_df = x_train_df.set_index('date')
        y_train_df = pd.DataFrame(y_train)
        y_train_df['date'] = pd.to_datetime(y_train_df['date'])
        y_train_df = y_train_df.set_index('date')
        
        expected_x_sequence = np.array([[[1000., 20., 2.1, 1.34, 2.06, 4.2, 3.05,
        2.04, 2.6, 4.2, 3.2, 3.6, 4.1, 2.5,
        2.75, 4.71, 7.05, -1., 25., 10., 3.,
        10., 5., 0., 500.0, 0.],
        [1000., 40., 2.05, 1.34, 2.06, 4.2, 3.05,
        2.6, 2.71, 6.19, 3.2, 3.6, 4.1, 2.5,
        2.75, 4.71, 7.05, -10., 30., 12., 4.,
        5., 3., 15., 500.0, 10.]],
        [[1000., 40., 2.05, 1.34, 2.06, 4.2, 3.05,
        2.6, 2.71, 6.19, 3.2, 3.6, 4.1, 2.5,
        2.75, 4.71, 7.05, -10., 30., 12., 4.,
        5., 3., 15., 500.0, 10.],
        [1000., 60., 2.04, 1.34, 2.06, 4.2, 3.05,
        2.45, 3.12, 2.5, 3.2, 3.6, 4.1, 2.5,
        2.75, 4.71, 7.05, -20., 40., 4., 6.,
        1., 0., 17., 1000.0, 20.]]])
        expected_y_sequence = np.array([[2.1],
        [2.08]])

        # Collect sequences from generator
        x_sequences, y_sequences = [], []
        for x_seq, y_next in EtlTransforms.generator(x=x_train_df, y=y_train_df, sequence_length=2):
            x_sequences.append(x_seq)
            y_sequences.append(y_next)
        
        # Convert to np.array
        x_sequences = np.array(x_sequences)
        y_sequences = np.array(y_sequences)

        # Assertions
        assert x_sequences.shape == (2, 2, 26)
        assert y_sequences.shape == (2, 1)
        np.testing.assert_array_equal(x_sequences, expected_x_sequence)
        np.testing.assert_array_equal(y_sequences, expected_y_sequence)
    
    def test_build_dataset(self):
        ''' Tests build_dataset function of EtlTransforms class '''
        x_train = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [1000, 1000, 1000, 2000],
        'lng_imports': [20, 40, 60, 80],
        'heating_oil_natural_gas_price_ratio': [2.1, 2.05, 2.04, 2.06],
        '7day_ew_volatility price ($/MMBTU)': [1.34, 1.34, 1.34, 1.34],
        '14day_ew_volatility price ($/MMBTU)': [2.06, 2.06, 2.06, 2.06],
        '30day_ew_volatility price ($/MMBTU)': [4.2, 4.2, 4.2, 4.2],
        '60day_ew_volatility price ($/MMBTU)': [3.05, 3.05, 3.05, 3.05],
        'price_1day_lag ($/MMBTU)': [2.04, 2.60, 2.45, 1.23],
        'price_2day_lag ($/MMBTU)': [2.60, 2.71, 3.12, 4.90],
        'price_3day_lag ($/MMBTU)': [4.20, 6.19, 2.50, 6.90],
        '7day_rolling_average price ($/MMBTU)': [3.20, 3.20, 3.20, 3.20],
        '14day_rolling_average price ($/MMBTU)': [3.60, 3.60, 3.60, 3.60],
        '30day_rolling_average price ($/MMBTU)': [4.10, 4.10, 4.10, 4.10],
        '7day_rolling_median price ($/MMBTU)': [2.50, 2.50, 2.50, 2.50],
        '14day_rolling_median price ($/MMBTU)': [2.75, 2.75, 2.75, 2.75],
        '30day_rolling_median price ($/MMBTU)': [4.71, 4.71, 4.71, 4.71],
        'total_consumption_total_underground_storage_ratio': [7.05, 7.05, 7.05, 7.05],
        'min_tavg': [-1, -10, -20, 3],
        'max_tavg': [25, 30, 40, 32],
        'max_abs_tavg_diff': [10, 12, 4, 6],
        'max_abs_tavg_diff_relative_to_daily_median': [3, 4, 6, 7],
        'hdd_max': [10, 5, 1, 2],
        'cdd_max': [5, 3, 0, 3],
        'wci_sum': [0, 15, 17, 10],
        'natural_gas_rigs_in_operation': [500, 500, 1000, 2000],
        'snow_sum': [0, 10, 20, 0]
        }
        y_train = {'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.10, 2.08]}
        x_train_df = pd.DataFrame(x_train)
        x_train_df['date'] = pd.to_datetime(x_train_df['date'])
        x_train_df = x_train_df.set_index('date')
        y_train_df = pd.DataFrame(y_train)
        y_train_df['date'] = pd.to_datetime(y_train_df['date'])
        y_train_df = y_train_df.set_index('date')
        train_dataset = EtlTransforms.build_dataset(x=x_train_df, y=y_train_df, sequence_length=30, batch_size=128)
        sample = train_dataset.take(1)
        for element in sample:
            assert np.array_equal(tf.shape(element[0]).numpy(), np.array([128, 30, 26]))
            assert np.array_equal(tf.shape(element[1]).numpy(), np.array([128, 1]))
    
    def test_normalisation(self):
        ''' Tests normalisation function of EtlTransforms class '''
        x_train = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [1000, 1000, 1000, 2000],
        'lng_imports': [20, 40, 60, 80],
        'heating_oil_natural_gas_price_ratio': [2.1, 2.05, 2.04, 2.06],
        '7day_ew_volatility price ($/MMBTU)': [1.34, 1.34, 1.34, 1.34],
        '14day_ew_volatility price ($/MMBTU)': [2.06, 2.06, 2.06, 2.06],
        '30day_ew_volatility price ($/MMBTU)': [4.2, 4.2, 4.2, 4.2],
        '60day_ew_volatility price ($/MMBTU)': [3.05, 3.05, 3.05, 3.05],
        'price_1day_lag ($/MMBTU)': [2.04, 2.60, 2.45, 1.23],
        'price_2day_lag ($/MMBTU)': [2.60, 2.71, 3.12, 4.90],
        'price_3day_lag ($/MMBTU)': [4.20, 6.19, 2.50, 6.90],
        '7day_rolling_average price ($/MMBTU)': [3.20, 3.20, 3.20, 3.20],
        '14day_rolling_average price ($/MMBTU)': [3.60, 3.60, 3.60, 3.60],
        '30day_rolling_average price ($/MMBTU)': [4.10, 4.10, 4.10, 4.10],
        '7day_rolling_median price ($/MMBTU)': [2.50, 2.50, 2.50, 2.50],
        '14day_rolling_median price ($/MMBTU)': [2.75, 2.75, 2.75, 2.75],
        '30day_rolling_median price ($/MMBTU)': [4.71, 4.71, 4.71, 4.71],
        'total_consumption_total_underground_storage_ratio': [7.05, 7.05, 7.05, 7.05],
        'min_tavg': [-1, -10, -20, 3],
        'max_tavg': [25, 30, 40, 32],
        'max_abs_tavg_diff': [10, 12, 4, 6],
        'max_abs_tavg_diff_relative_to_daily_median': [3, 4, 6, 7],
        'hdd_max': [10, 5, 1, 2],
        'cdd_max': [5, 3, 0, 3],
        'wci_sum': [0, 15, 17, 10],
        'natural_gas_rigs_in_operation': [500, 500, 1000, 2000],
        'snow_sum': [0, 10, 20, 0]
        }
        x_test = {
        'date': ['1999-03-31', '1999-04-01'],
        'imports': [1000, 2000],
        'lng_imports': [60, 80],
        'heating_oil_natural_gas_price_ratio': [2.04, 2.06],
        '7day_ew_volatility price ($/MMBTU)': [1.34, 1.34],
        '14day_ew_volatility price ($/MMBTU)': [2.06, 2.06],
        '30day_ew_volatility price ($/MMBTU)': [4.2, 4.2],
        '60day_ew_volatility price ($/MMBTU)': [3.05, 3.05],
        'price_1day_lag ($/MMBTU)': [2.45, 1.23],
        'price_2day_lag ($/MMBTU)': [3.12, 4.90],
        'price_3day_lag ($/MMBTU)': [2.50, 6.90],
        '7day_rolling_average price ($/MMBTU)': [3.20, 3.20],
        '14day_rolling_average price ($/MMBTU)': [3.60, 3.60],
        '30day_rolling_average price ($/MMBTU)': [4.10, 4.10],
        '7day_rolling_median price ($/MMBTU)': [2.50, 2.50],
        '14day_rolling_median price ($/MMBTU)': [2.75, 2.75],
        '30day_rolling_median price ($/MMBTU)': [4.71, 4.71],
        'total_consumption_total_underground_storage_ratio': [7.05, 7.05],
        'min_tavg': [-20, 3],
        'max_tavg': [40, 32],
        'max_abs_tavg_diff': [4, 6],
        'max_abs_tavg_diff_relative_to_daily_median': [6, 7],
        'hdd_max': [1, 2],
        'cdd_max': [0, 3],
        'wci_sum': [17, 10],
        'natural_gas_rigs_in_operation': [1000, 2000],
        'snow_sum': [20, 0]
        }
        x_train_df = pd.DataFrame(x_train)
        x_train_df['date'] = pd.to_datetime(x_train_df['date'])
        x_train_df = x_train_df.set_index('date')
        x_test_df = pd.DataFrame(x_test)
        x_test_df['date'] = pd.to_datetime(x_test_df['date'])
        x_test_df = x_test_df.set_index('date')
        train_df_normalised, test_df_normalised = EtlTransforms.normalise(train_df=x_train_df, test_df=x_test_df)
        expected_train_data_normalised = {
        'date': ['1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'imports': [0.0, 0.0, 0.0, 4.0],
        'lng_imports': [-1.0, -0.3333333333333333, 0.3333333333333333, 1.0],
        'heating_oil_natural_gas_price_ratio': [1.9999999999999802, -0.22222222222221344, -0.6666666666666403, 0.2222222222222332],
        '7day_ew_volatility price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '14day_ew_volatility price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '30day_ew_volatility price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '60day_ew_volatility price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        'price_1day_lag ($/MMBTU)': [-0.3153846153846153, 0.5461538461538459, 0.3153846153846153, -1.5615384615384609],
        'price_2day_lag ($/MMBTU)': [-0.35694050991501397, -0.23229461756373937, 0.23229461756373937, 2.249291784702549],
        'price_3day_lag ($/MMBTU)': [-0.3837994214079074, 0.3837994214079074, -1.0395371263259403, 0.6576663452266152],
        '7day_rolling_average price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '14day_rolling_average price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '30day_rolling_average price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '7day_rolling_median price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '14day_rolling_median price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        '30day_rolling_median price ($/MMBTU)': [0.0, 0.0, 0.0, 0.0],
        'total_consumption_total_underground_storage_ratio': [0.0, 0.0, 0.0, 0.0],
        'min_tavg': [0.36, -0.36, -1.16, 0.68],
        'max_tavg': [-1.1428571428571428, -0.19047619047619047, 1.7142857142857142, 0.19047619047619047],
        'max_abs_tavg_diff': [0.4, 0.8, -0.8, -0.4],
        'max_abs_tavg_diff_relative_to_daily_median': [-0.8, -0.4, 0.4, 0.8],
        'hdd_max': [1.4444444444444444, 0.3333333333333333, -0.5555555555555556, -0.3333333333333333],
        'cdd_max': [1.6, 0.0, -2.4, 0.0],
        'wci_sum': [-1.5625, 0.3125, 0.5625, -0.3125],
        'natural_gas_rigs_in_operation': [-0.333333, -0.333333, 0.333333, 1.666667],
        'snow_sum': [0.0, 2.3978952727983707, 3.044522437723423, 0.0]
        }
        expected_train_df = pd.DataFrame(expected_train_data_normalised)
        expected_train_df['date'] = pd.to_datetime(expected_train_df['date'])
        expected_train_df = expected_train_df.set_index('date')
        expected_test_data_normalised = {
        'date': ['1999-03-31', '1999-04-01'],
        'imports': [0.0, 4.0],
        'lng_imports' : [0.3333333333333333, 1.0],
        'heating_oil_natural_gas_price_ratio': [-0.6666666666666403, 0.2222222222222332],
        '7day_ew_volatility price ($/MMBTU)': [0.0, 0.0],
        '14day_ew_volatility price ($/MMBTU)': [0.0, 0.0],
        '30day_ew_volatility price ($/MMBTU)': [0.0, 0.0],
        '60day_ew_volatility price ($/MMBTU)': [0.0, 0.0],
        'price_1day_lag ($/MMBTU)': [0.3153846153846153, -1.5615384615384609],
        'price_2day_lag ($/MMBTU)': [0.23229461756373937, 2.249291784702549],
        'price_3day_lag ($/MMBTU)': [-1.0395371263259403, 0.6576663452266152],
        '7day_rolling_average price ($/MMBTU)': [0.0, 0.0],
        '14day_rolling_average price ($/MMBTU)': [0.0, 0.0],
        '30day_rolling_average price ($/MMBTU)': [0.0, 0.0],
        '7day_rolling_median price ($/MMBTU)': [0.0, 0.0],
        '14day_rolling_median price ($/MMBTU)': [0.0, 0.0],
        '30day_rolling_median price ($/MMBTU)': [0.0, 0.0],
        'total_consumption_total_underground_storage_ratio': [0.0, 0.0],
        'min_tavg': [-1.16, 0.68],
        'max_tavg': [1.7142857142857142, 0.19047619047619047],
        'max_abs_tavg_diff': [-0.8, -0.4],
        'max_abs_tavg_diff_relative_to_daily_median': [0.4, 0.8],
        'hdd_max': [-0.5555555555555556, -0.3333333333333333],
        'cdd_max': [-2.4, 0.0],
        'wci_sum': [0.5625, -0.3125],
        'natural_gas_rigs_in_operation': [0.333333, 1.666667],
        'snow_sum': [3.044522437723423, 0.0]
        }
        expected_test_df = pd.DataFrame(expected_test_data_normalised)
        expected_test_df['date'] = pd.to_datetime(expected_test_df['date'])
        expected_test_df = expected_test_df.set_index('date')
        pd.testing.assert_frame_equal(train_df_normalised, expected_train_df)
        pd.testing.assert_frame_equal(test_df_normalised, expected_test_df)
    
    def test_calculate_moving_average(self, calculate_rolling_variables_df):
        ''' Tests calculate_moving_average function of EtlTransforms class '''
        expected_average = 3
        calculated_average = EtlTransforms.calculate_moving_average(df=calculate_rolling_variables_df, window=5)
        assert calculated_average == expected_average
    
    def test_calculate_rolling_median(self, calculate_rolling_variables_df):
        ''' Tests calculate_rolling_median function of EtlTransforms class '''
        expected_median = 3
        calculated_median = EtlTransforms.calculate_rolling_median(df=calculate_rolling_variables_df, window=5)
        assert calculated_median == expected_median
    
    def test_calculate_ew_volatility(self, calculate_rolling_variables_df):
        ''' Tests calculate_ew_volatility function of EtlTransforms class '''
        expected_ew_volatility = 1.1416241873418624
        calculated_ew_volatility = EtlTransforms.calculate_ew_volatility(df=calculate_rolling_variables_df, window=2)
        assert calculated_ew_volatility == expected_ew_volatility














    
    






    

    
    
        
        







        




    
        


