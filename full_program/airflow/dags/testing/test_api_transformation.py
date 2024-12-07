''' Import modules '''
import pytest
import pandas as pd
from dags.transformation.noaa_api_transformation import *
from dags.transformation.eia_api_transformation import *
from dags.fixtures.fixtures import df_noaa_transformation_testing, df_noaa_transformation_testing_impute_missing_weather_variables, df_noaa_feature_engineering_testing, \
df_convert_column_to_float, df_eia_feature_engineering_testing

class TestNoaaTransformation:
    ''' 
    Test class for testing NoaaTransformation class 
    '''
    def test_modify_date(self, df_noaa_transformation_testing):
        ''' 
        Tests modify_date function of NoaaTransformation class
        '''
        expected_quarters = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 4]
        df = NoaaTransformation.modify_date(df=df_noaa_transformation_testing)
        assert pd.api.types.is_datetime64_any_dtype(df['date'])
        assert df['quarter'].tolist() == expected_quarters
        assert df.shape == (17, 7)

    def test_imputation_df(self, df_noaa_transformation_testing):
        '''
        Tests imputation_df function of NoaaTransformation class
        '''
        df_noaa_transformation_testing = NoaaTransformation.modify_date(df=df_noaa_transformation_testing)
        data = [{'city': 'Detroit' , 'quarter': 1, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': -2},
        {'city': 'Detroit' , 'quarter': 1, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': 5},
        {'city': 'Detroit' , 'quarter': 1, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': 4.17},
        {'city': 'Detroit' , 'quarter': 1, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0.1},
        {'city': 'Detroit' , 'quarter': 2, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 2, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 2, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 2, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'Detroit' , 'quarter': 3, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 3, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 3, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 3, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'Detroit' , 'quarter': 4, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 4, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 4, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Detroit' , 'quarter': 4, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': -10},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': -3},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': 5},
        {'city': 'Chicago' , 'quarter': 1, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0.2},
        {'city': 'Chicago' , 'quarter': 2, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 2, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 2, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 2, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'Chicago' , 'quarter': 3, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 3, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 3, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 3, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'Chicago' , 'quarter': 4, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 4, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 4, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'Chicago' , 'quarter': 4, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'New York' , 'quarter': 1, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 1, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 1, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 1, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 1},
        {'city': 'New York' , 'quarter': 2, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': -5},
        {'city': 'New York' , 'quarter': 2, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 2, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 2, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'New York' , 'quarter': 3, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 3, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': -1},
        {'city': 'New York' , 'quarter': 3, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 3, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0},
        {'city': 'New York' , 'quarter': 4, 'datatype': 'TMIN', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 4, 'datatype': 'TMAX', 'impute method': 'Mean', 'impute value': None},
        {'city': 'New York' , 'quarter': 4, 'datatype': 'AWND', 'impute method': 'Mean', 'impute value': 3.1},
        {'city': 'New York' , 'quarter': 4, 'datatype': 'SNOW', 'impute method': 'Median', 'impute value': 0}
        ]
        expected_df = pd.DataFrame(data)
        result_df = NoaaTransformation.imputation_df(df=df_noaa_transformation_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_impute_missing_weather_variables(self, df_noaa_transformation_testing_impute_missing_weather_variables):
        ''' 
        Tests impute_missing_weather_variables function of NoaaTransformation class
        '''
        df_noaa_transformation_testing_impute_missing_weather_variables = NoaaTransformation.modify_date(df=df_noaa_transformation_testing_impute_missing_weather_variables)
        data = [{'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': None, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 0, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1}]
        expected_df = pd.DataFrame(data)
        expected_df = NoaaTransformation.modify_date(df=expected_df)
        imputation_df = NoaaTransformation.imputation_df(df=df_noaa_transformation_testing_impute_missing_weather_variables)
        result_df = NoaaTransformation.impute_missing_weather_variables(df=df_noaa_transformation_testing_impute_missing_weather_variables, imputation_df=imputation_df)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_calculate_missing_tavg(self, df_noaa_transformation_testing):
        '''
        Tests calculate_missing_tavg function of NoaaTransformation class
        '''
        df_noaa_transformation_testing = NoaaTransformation.modify_date(df=df_noaa_transformation_testing)
        data = [{'date': '1999-01-04', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.3, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '1999-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.2, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2000-01-02', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 4.0, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -2, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '1999-01-04', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.4, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1},
        {'date': '1999-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.2, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1},
        {'date': '2000-01-02', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 0.1, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -10, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': -3, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 5, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'SNOW', 'station': 'GHCND:USW00094847', 'value': 1, 'city': 'New York', 'state': 'New York', 'quarter': 1},
        {'date': '2001-04-05', 'datatype': 'TMIN', 'station': 'GHCND:USW00094847', 'value': -5, 'city': 'New York', 'state': 'New York', 'quarter': 2},
        {'date': '2001-07-05', 'datatype': 'TMAX', 'station': 'GHCND:USW00094847', 'value': -1, 'city': 'New York', 'state': 'New York', 'quarter': 3},
        {'date': '2001-10-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': -3, 'city': 'New York', 'state': 'New York', 'quarter': 4},
        {'date': '2001-10-05', 'datatype': 'AWND', 'station': 'GHCND:USW00094847', 'value': 3.1, 'city': 'New York', 'state': 'New York', 'quarter': 4},
        {'date': '2001-01-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': 1.5, 'city': 'Detroit', 'state': 'Michigan', 'quarter': 1},
        {'date': '2001-01-05', 'datatype': 'TAVG', 'station': 'GHCND:USW00094847', 'value': -6.5, 'city': 'Chicago', 'state': 'Illinois', 'quarter': 1}]
        expected_df = pd.DataFrame(data)
        expected_df = NoaaTransformation.modify_date(df=expected_df)
        result_df = NoaaTransformation.calculate_missing_tavg(df=df_noaa_transformation_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_maximum_hdd(self, df_noaa_feature_engineering_testing):
        '''
        Tests maximum_hdd function of NooaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'hdd_max': [18.33, 18.33, 18.33, 18.33, 18.33, 18.33, 2.33, 2.33, 2.33, 1.33, 1.33, 1.33, 2.33, 2.33, 2.33, 21.33, 21.33, 21.33, 18.33, 18.33, 18.33, 28.33, 28.33, 28.33, 33.33, 33.33, 33.33]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.maximum_hdd(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_maximum_cdd(self, df_noaa_feature_engineering_testing):
        '''
        Tests maximum_cdd function of NooaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'cdd_max': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11.67, 11.67, 11.67, 0, 0, 0, 0, 0, 0, 6.67, 6.67, 6.67, 3.67, 3.67, 3.67]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.maximum_cdd(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_wci_sum(self, df_noaa_feature_engineering_testing):
        '''
        Tests wci_sum function of NoaaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'wci_sum': [141.12, 141.12, 141.12, 129.50, 129.50, 129.50, 180.37, 180.37, 180.37, 202.29, 202.29, 202.29, 212.65, 212.65, 212.65, 103.57, 103.57,
        103.57, 97.29, 97.29, 97.29, 74.66, 74.66, 74.66, 59.08, 59.08, 59.08]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.wci_sum(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_snow_sum(self, df_noaa_feature_engineering_testing):
        '''
        Tests snow_sum function of NoaaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'snow_sum': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 3, 3, 3, 12, 12, 12]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.snow_sum(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_min_and_max_average_temperature(self, df_noaa_feature_engineering_testing):
        '''
        Tests min_and_max_average_temperature function of NoaaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'min_tavg': [0.1, 0.1, 0.1, 0, 0, 0, 16, 16, 16, 17, 17, 17, 16, 16, 16, -3, -3, -3, 0, 0, 0, -10, -10, -10, -15, -15, -15],
        'max_tavg': [17.1, 17.1, 17.1, 15, 15, 15, 17, 17, 17, 17, 17, 17, 30, 30, 30, 15, 15, 15, 7, 7, 7, 25, 25, 25, 22, 22, 22]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.min_and_max_average_temperature(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_max_abs_tavg_diff(self, df_noaa_feature_engineering_testing):
        '''
        Tests max_abs_tavg_diff function of NoaaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'max_abs_tavg_diff': [None, None, None, 2.1, 2.1, 2.1, 17, 17, 17, 1, 1, 1, 13, 13, 13, 19, 19, 19, 15, 15, 15, 19, 19, 19, 5, 5, 5]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.max_abs_tavg_diff(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_max_abs_tavg_diff_relative_daily_median(self, df_noaa_feature_engineering_testing):
        '''
        Tests max_abs_tavg_diff_relative_daily_median function of NoaaTransformation class
        '''
        data = {'date': ['1999-01-05', '1999-01-05', '1999-01-05', '1999-01-06', '1999-01-06', '1999-01-06', '1999-01-07', '1999-01-07', '1999-01-07',
        '2000-01-05', '2000-01-05', '2000-01-05', '2000-01-06', '2000-01-06', '2000-01-06', '2000-01-07', '2000-01-07', '2000-01-07', 
        '2004-01-05', '2004-01-05', '2004-01-05', '2004-01-06', '2004-01-06', '2004-01-06', '2004-01-07', '2004-01-07', '2004-01-07'],
        'state': ['Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York',
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 
        'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York', 'Florida', 'Illinois', 'New York'],
        'city': ['Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York',
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 
        'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York', 'Miami', 'Chicago', 'New York'],
        'awnd': [10, 5, 1, 8, 7, 4, 2, 10, 4, 0, 2, 1, 3, 4, 2, 1, 5, 3, 1, 15, 8, 3, 17, 12, 4, 6, 7],
        'snow': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 3, 0, 2, 1, 0, 6, 4, 2],
        'tavg': [0.1, 15, 17.1, 0, 13, 15, 17, 16, 16, 17, 17, 17, 16, 17, 30, -3, -1, 15, 6, 7, 0, 25, -10, -6, 22, -15, -10],
        'max_abs_tavg_diff_relative_to_daily_median': [8.45, 8.45, 8.45, 8.0, 8.0, 8.0, 10.0, 10.0, 10.0, 8.45, 8.45, 8.45, 8.0, 8.0, 8.0, 10.0, 10.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = NoaaTransformation.max_abs_tavg_diff_relative_to_daily_median(df=df_noaa_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)

class TestEiaTransformation:
    ''' 
    Test class for testing EiaTransformation class 
    '''
    def test_convert_column_to_float(self, df_convert_column_to_float):
        '''
        Tests convert_price_to_float function of EiaTransformation class
        '''
        data = {'period': ['1999-01', '1999-02', '1999-03', '1999-04'],
        'duoarea': ['NUS-Z00', 'NUS-Z00', 'NUS-Z00', 'NUS-Z00'],
        'area-name': ['US.', 'US.', 'US.', 'US.'],
        'product': ['EPG0', 'EPG0', 'EPG0', 'EPG0'],
        'product-name': ['Natural Gas', 'Natural Gas', 'Natural Gas', 'Natural Gas'],
        'process': ['PML', 'PML', 'PML', 'PML'],
        'process-name': ['Imports', 'Imports', 'Imports', 'Imports'],
        'series': ['N9103US3', 'N9103US3', 'N9103US3', 'N9103US3'],
        'series-description': ['Total US Natural Gas Imports', 'Total US Natural Gas Imports', 'Total US Natural Gas Imports', 'Total US Natural Gas Imports'],
        'value': [3.0, 4.2, 5.1, 6.2],
        'units': ['$/MCF', '$/MCF', '$/MCF', '$/MCF']}
        expected_df = pd.DataFrame(data)
        result_df = EiaTransformation.convert_column_to_float(df=df_convert_column_to_float, column='value')
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_natural_gas_prices_lag(self, df_eia_feature_engineering_testing):
        '''
        Tests natural_gas_prices_lag function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        'price_1day_lag ($/MMBTU)': [None, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02],
        'price_2day_lag ($/MMBTU)': [None, None, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89],
        'price_3day_lag ($/MMBTU)': [None, None, None, 2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.natural_gas_prices_lag(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_heating_oil_to_natural_gas_price_ratio(self, df_eia_feature_engineering_testing):
        '''
        Tests heating_oil_to_natural_gas_price_ratio function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        'heating_oil_natural_gas_price_ratio' : [0.16, 0.16, 0.17, 0.18, 0.19, 0.2, 0.19, 0.18, 0.18, 0.18, 0.18, 0.17, 0.17, 0.18, 0.18, 0.18, 0.19, 0.19, 0.18, 0.18, 0.18, 0.18, 0.17, 0.17, 0.16, 0.16, 0.16, 0.17, 0.16, 0.16, 0.16, 0.17, 0.16, 0.17, 0.18, 0.19, 0.2, 0.2, 0.19, 0.19, 0.2, 0.2, 0.2, 0.19, 0.19, 0.2, 0.2, 0.21, 0.22, 0.22, 0.23, 0.23, 0.24, 0.23, 0.23, 0.23, 0.23, 0.24, 0.24, 0.24, 0.22, 0.22]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.heating_oil_to_natural_gas_price_ratio(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_expotential_weighted_natural_gas_price_volatility(self, df_eia_feature_engineering_testing):
        '''
        Tests expotential_weighted_natural_gas_price_volatility function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        '7day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, 0.1, 0.09, 0.09, 0.09, 0.08, 0.07, 0.06, 0.05, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.01, 0.02, 0.02, 0.03, 0.07, 0.07, 0.07, 0.06, 0.05, 0.05, 0.05, 0.09, 0.09, 0.11, 0.1, 0.09, 0.08, 0.08, 0.07, 0.06, 0.06, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.05, 0.11, 0.1],
        '14day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04, 0.04, 0.04, 0.04, 0.03, 0.03, 0.03, 0.03, 0.03, 0.04, 0.06, 0.07, 0.07, 0.07, 0.07, 0.06, 0.06, 0.08, 0.08, 0.1, 0.1, 0.09, 0.09, 0.08, 0.08, 0.07, 0.07, 0.07, 0.06, 0.06, 0.06, 0.06, 0.05, 0.06, 0.1, 0.1],
        '30day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 0.06, 0.06, 0.06, 0.05, 0.05, 0.05, 0.05, 0.07, 0.07, 0.08, 0.08, 0.08, 0.08, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.08, 0.08, 0.08, 0.08, 0.07, 0.07, 0.07, 0.07, 0.07, 0.06, 0.07, 0.09, 0.09],
        '60day_ew_volatility price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 0.07, 0.09, 0.09]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.expotential_weighted_natural_gas_price_volatility(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_rolling_average_natural_gas_price(self, df_eia_feature_engineering_testing):
        '''
        Tests rolling_average_natural_gas_price function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        '7day_rolling_average price ($/MMBTU)': [None, None, None, None, None, None, 1.95, 1.92, 1.88, 1.84, 1.82, 1.81, 1.81, 1.81, 1.79, 1.79, 1.78, 1.78, 1.78, 1.77, 1.76, 1.77, 1.78, 1.79, 1.8, 1.79, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.8, 1.79, 1.79, 1.77, 1.75, 1.73, 1.71, 1.69, 1.68, 1.67, 1.68, 1.71, 1.74, 1.78, 1.81, 1.83, 1.83, 1.84, 1.82, 1.8, 1.77, 1.75, 1.75, 1.76, 1.77, 1.78, 1.78, 1.81, 1.85, 1.87],
        '14day_rolling_average price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, 1.88, 1.86, 1.83, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.78, 1.79, 1.79, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.78, 1.77, 1.75, 1.74, 1.74, 1.73, 1.72, 1.73, 1.74, 1.75, 1.75, 1.75, 1.75, 1.76, 1.76, 1.77, 1.78, 1.78, 1.79, 1.8, 1.8, 1.8, 1.79, 1.79, 1.8, 1.81],
        '30day_rolling_average price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 1.83, 1.82, 1.81, 1.8, 1.8, 1.79, 1.79, 1.78, 1.78, 1.77, 1.77, 1.76, 1.76, 1.76, 1.76, 1.76, 1.77, 1.77, 1.78, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.77, 1.78, 1.78]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.rolling_average_natural_gas_price(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_rolling_median_natural_gas_price(self, df_eia_feature_engineering_testing):
        '''
        Tests rolling_median_natural_gas_price function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        '7day_rolling_median price ($/MMBTU)': [None, None, None, None, None, None, 1.91, 1.9, 1.87, 1.83, 1.82, 1.81, 1.81, 1.81, 1.78, 1.78, 1.77, 1.76, 1.76, 1.75, 1.75, 1.75, 1.78, 1.79, 1.8, 1.8, 1.8, 1.8, 1.81, 1.81, 1.8, 1.8, 1.79, 1.79, 1.79, 1.79, 1.77, 1.75, 1.73, 1.67, 1.67, 1.67, 1.67, 1.68, 1.72, 1.74, 1.86, 1.86, 1.86, 1.86, 1.81, 1.75, 1.75, 1.75, 1.75, 1.75, 1.75, 1.79, 1.8, 1.8, 1.8, 1.83],
        '14day_rolling_median price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, 1.84, 1.83, 1.82, 1.82, 1.80, 1.80, 1.78, 1.78, 1.78, 1.78, 1.78, 1.80, 1.80, 1.80, 1.78, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80, 1.79, 1.79, 1.79, 1.78, 1.78, 1.76, 1.74, 1.74, 1.74, 1.74, 1.74, 1.74, 1.74, 1.74, 1.74, 1.75, 1.75, 1.75, 1.75, 1.75, 1.77, 1.80, 1.80, 1.80, 1.80, 1.80, 1.80],
        '30day_rolling_median price ($/MMBTU)': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 1.81, 1.81, 1.8, 1.8, 1.80, 1.79, 1.79, 1.79, 1.78, 1.78, 1.78, 1.78, 1.78, 1.78, 1.78, 1.78, 1.78, 1.79, 1.79, 1.79, 1.79, 1.79, 1.78, 1.78, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76, 1.76]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.rolling_median_natural_gas_price(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_total_consumption_to_total_underground_storage_ratio(self, df_eia_feature_engineering_testing):
        '''
        Tests total_consumption_to_total_underground_storage_ratio function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        'total_consumption_total_underground_storage_ratio': [0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.22, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.18, 0.12]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.total_consumption_to_total_underground_storage_ratio(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    def test_is_december_or_january(self, df_eia_feature_engineering_testing):
        '''
        Tests is_december_or_january function of EiaTransformation class
        '''
        data = {'date': ['1999-01-04', '1999-01-05', '1999-01-06', '1999-01-07', '1999-01-08', '1999-01-11', '1999-01-12', '1999-01-13',
        '1999-01-14', '1999-01-15', '1999-01-19', '1999-01-20', '1999-01-21', '1999-01-22', '1999-01-25', '1999-01-26',
        '1999-01-27', '1999-01-28', '1999-01-29', '1999-02-01', '1999-02-02', '1999-02-03', '1999-02-04', '1999-02-05',
        '1999-02-08', '1999-02-09', '1999-02-10', '1999-02-11', '1999-02-12', '1999-02-16', '1999-02-17', '1999-02-18',
        '1999-02-19', '1999-02-22', '1999-02-23', '1999-02-24', '1999-02-25', '1999-02-26', '1999-03-01', '1999-03-02',
        '1999-03-03', '1999-03-04', '1999-03-05', '1999-03-08', '1999-03-09', '1999-03-10', '1999-03-11', '1999-03-12',
        '1999-03-15', '1999-03-16', '1999-03-17', '1999-03-18', '1999-03-19', '1999-03-22', '1999-03-23', '1999-03-24',
        '1999-03-25', '1999-03-26', '1999-03-29', '1999-03-30', '1999-03-31', '1999-04-01'],
        'price ($/MMBTU)': [2.1, 2.05, 2.04, 1.91, 1.9, 1.83, 1.82, 1.87, 1.77, 1.78, 1.77, 1.81, 1.85, 1.82, 1.76, 1.73, 1.75, 1.75, 1.83, 1.75, 1.78, 1.8, 1.79, 1.81, 1.81, 1.82, 1.8, 1.78, 1.82, 1.79, 1.79, 1.8, 1.79, 1.77, 1.75, 1.73, 1.64, 1.63, 1.65, 1.67, 1.68, 1.72, 1.74, 1.87, 1.86, 1.94, 1.87, 1.81, 1.75, 1.75, 1.75, 1.75, 1.73, 1.74, 1.8, 1.79, 1.8, 1.83, 1.8, 1.89, 2.02, 1.95],
        'price_heating_oil ($/GAL)': [0.346, 0.338, 0.354, 0.352, 0.36, 0.371, 0.352, 0.337, 0.327, 0.322, 0.323, 0.314, 0.323, 0.328, 0.322, 0.316, 0.327, 0.327, 0.329, 0.32, 0.318, 0.319, 0.304, 0.3, 0.294, 0.294, 0.297, 0.296, 0.293, 0.284, 0.287, 0.298, 0.291, 0.304, 0.314, 0.323, 0.327, 0.323, 0.319, 0.325, 0.333, 0.345, 0.349, 0.358, 0.36, 0.386, 0.377, 0.389, 0.383, 0.388, 0.396, 0.399, 0.408, 0.408, 0.418, 0.409, 0.418, 0.432, 0.433, 0.445, 0.442, 0.436],
        'residential_consumption': [911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 911162.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 689687.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 669270.0, 420192.0],
        'commercial_consumption': [475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 475945.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 400207.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 388912.0, 259432.0],
        'total_underground_storage': [6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6404470.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 6074901.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5788960.0, 5876197.0],
        'is_dec_or_jan': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}
        expected_df = pd.DataFrame(data)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df = expected_df.set_index('date')
        result_df = EiaTransformation.is_december_or_january(df=df_eia_feature_engineering_testing)
        pd.testing.assert_frame_equal(result_df, expected_df)
    
    

    

    



    




    
    

    

    










