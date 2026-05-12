# Import modules
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import numpy as np
import re

class DataQualityChecks:
    @staticmethod
    def is_numeric_or_null(element: int | float | str):
        ''' 
        Data quality check to ensure a str, int or float column is numeric or null

        Args:
            element (int | float | str): DataFrame column value at row level
        '''
        if pd.isna(element):
            return True
        if isinstance(element, (int, float)):
            return True
        if isinstance(element, str):
            return bool(re.fullmatch(r"-?(\d+(\.\d+)?|\.\d+)", element))
        return False
    
    @staticmethod
    def is_yyyy_mm_dd(element: str):
        '''
        Data quality check to ensure a str date column matches yyyy-mm-dd format

        Args:
            element (str): Date column value at row level in DataFrame
        '''
        return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", element))
    
    @staticmethod
    def check_is_string(element):
        '''
        Data quality check to ensure a column is of str type

        Args:
            element (str): Column value at row level in DataFrame
        '''
        return isinstance(element,str)
