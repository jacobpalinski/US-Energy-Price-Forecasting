''' Import modules '''
import pandas as pd

class EiaTransformation:
    '''
    Class for performing transformations on data extracted from EIA API

    Methods
    -------
    convert_column_to_float(cls, df, column):
        Converts price column from non-float format to floating format
    convert_date_format(cls, df):
        Convert YYYY-MM date format to YYYY-MM-DD for monthly variables extracted
    natural_gas_prices_lag(cls, df):
        Creates lag variables for 1,2 and 3 days for natural gas prices
    heating_oil_natural_gas_price_ratio(cls, df):
        Creates a ratio of price of heating oil vs price of natural gas
    expotential_weighted_natural_gas_price_volatility(cls, df):
        Calculates expotential weighted natural gas price volatility for 7, 14, 30 and 60 days
    rolling_average_natural_gas_price(cls, df):
        Creates rolling average of natural gas prices for 7, 14 and 30 days
    rolling_median_natural_gas_price(cls, df):
        Creates rolling median of natural gas prices for 7, 14 and 30 days
    total_consumption_to_total_underground_storage_ratio(cls, df):
        Creates total natural gas consumption to natural gas underground storage ratio
    is_december_or_january(cls, df):
        Creates is_dec_or_jan binary variable where 1 indicates a given date is in December or January
    '''
    @classmethod
    def convert_column_to_float(cls, df: pd.DataFrame, column: str) -> pd.DataFrame:
        '''
        Converts price column from non-float format to floating format

        Args:
            df (pd.DataFrame): Natural gas / heating oil prices df
            column (str): Name of price column to be converted to float format
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df[column] = pd.to_numeric(df[column])
        return df
    
    @classmethod
    def convert_date_format(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Convert YYYY-MM date format to YYYY-MM-DD for monthly variables extracted

        Args:
            df (pd.DataFrame): Dataframe where date format is going to be modified

        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m').dt.strftime('%Y-%m-%d')
        return df

    @classmethod
    def natural_gas_prices_lag(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates lag variables for 1,2 and 3 days for natural gas prices
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe 
        '''
        df['price_1day_lag ($/MMBTU)'] = df['price ($/MMBTU)'].shift(1)
        df['price_2day_lag ($/MMBTU)'] = df['price ($/MMBTU)'].shift(2)
        df['price_3day_lag ($/MMBTU)'] = df['price ($/MMBTU)'].shift(3)
        return df
    
    @classmethod
    def heating_oil_to_natural_gas_price_ratio(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates a ratio of price of heating oil vs price of natural gas
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe 
        '''
        df['heating_oil_natural_gas_price_ratio'] = round(df['price_heating_oil ($/GAL)'] / df['price ($/MMBTU)'], 2)
        return df

    @classmethod
    def expotential_weighted_natural_gas_price_volatility(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates expotential weighted natural gas price volatility for 7, 14, 30 and 60 days
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe 
        '''
        df['7day_ew_volatility price ($/MMBTU)'] = round(df['price ($/MMBTU)'].ewm(span=7, min_periods=7).std(), 2)
        df['14day_ew_volatility price ($/MMBTU)'] = round(df['price ($/MMBTU)'].ewm(span=14, min_periods=14).std(), 2)
        df['30day_ew_volatility price ($/MMBTU)'] = round(df['price ($/MMBTU)'].ewm(span=30, min_periods=30).std(), 2)
        df['60day_ew_volatility price ($/MMBTU)'] = round(df['price ($/MMBTU)'].ewm(span=60, min_periods=60).std(), 2)
        return df
    
    @classmethod
    def rolling_average_natural_gas_price(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates rolling average of natural gas prices for 7, 14 and 30 days
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe 
        '''
        df['7day_rolling_average price ($/MMBTU)'] = round(df['price ($/MMBTU)'].rolling(window=7, min_periods=7).mean(), 2)
        df['14day_rolling_average price ($/MMBTU)'] = round(df['price ($/MMBTU)'].rolling(window=14, min_periods=14).mean(), 2)
        df['30day_rolling_average price ($/MMBTU)'] = round(df['price ($/MMBTU)'].rolling(window=30, min_periods=30).mean(), 2)
        return df
    
    @classmethod
    def rolling_median_natural_gas_price(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates rolling median of natural gas prices for 7, 14 and 30 days
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe 
        '''
        df['7day_rolling_median price ($/MMBTU)'] = round(df['price ($/MMBTU)'].rolling(window=7, min_periods=7).median(), 2)
        df['14day_rolling_median price ($/MMBTU)'] = round(df['price ($/MMBTU)'].rolling(window=14, min_periods=14).median(), 2)
        df['30day_rolling_median price ($/MMBTU)'] = round(df['price ($/MMBTU)'].rolling(window=30, min_periods=30).median(), 2)
        return df
    
    @classmethod
    def total_consumption_to_total_underground_storage_ratio(cls, df: pd.DataFrame) -> pd.DataFrame:
        ''' 
        Creates total natural gas consumption to natural gas underground storage ratio
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe 
        '''
        df['total_consumption_total_underground_storage_ratio'] = round((df['residential_consumption'] + df['commercial_consumption']) / df['total_underground_storage'], 2)
        return df

    @classmethod
    def is_december_or_january(cls, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Creates is_dec_or_jan binary variable where 1 indicates a given date is in December or January
        
        Args:
            df (pd.DataFrame): Natural gas prices df
        
        Returns:
            pd.DataFrame: Returns modified dataframe
        '''
        df['is_dec_or_jan'] = (df.index.month == 12) | (df.index.month == 1)
        df['is_dec_or_jan'] = df['is_dec_or_jan'].astype(int)
        return df




    



    


