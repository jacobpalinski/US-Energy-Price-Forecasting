# Import modules
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import numpy as np
import re

class DataQualityChecks:
    @staticmethod
    def is_numeric_or_null(element):
        if pd.isna(element):
            return True
        if isinstance(element, (int, float)):
            return True
        if isinstance(element, str):
            return bool(re.fullmatch(r"-?(\d+(\.\d+)?|\.\d+)", element))
        return False
    
    @staticmethod
    def is_yyyy_mm_dd(element):
        return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", element))
    
    @staticmethod
    def check_is_string(element):
        return isinstance(element,str)
