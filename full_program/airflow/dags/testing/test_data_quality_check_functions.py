import pytest
import pandas as pd
import numpy as np
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from dags.utils.data_quality_check_functions import DataQualityChecks

class TestDataQualityCheckFunctions:
    def test_is_numeric_or_null_valid_values_pass_schema(self):
        ''' Test that valid numeric and null values pass the schema validation '''
        df = pd.DataFrame(
            {"value": [None, np.nan, 0, 10, -5, 3.14, "0", "-10", "2.71"]}
        )

        schema = DataFrameSchema(
        {
            "value": Column(
                pa.Object,
                checks=Check(
                    DataQualityChecks.is_numeric_or_null,
                    element_wise=True,
                ),
                nullable=True,
            )
        }
        )

        schema.validate(df)
    
    def test_is_numeric_or_null_invalid_values_fail_schema(self):
        ''' Test that invalid numeric and null values fail the schema validation '''
        df = pd.DataFrame(
            {"value": ["abc", "1a", "3.14.15", []]}
        )

        schema = DataFrameSchema(
        {
            "value": Column(
                pa.Object,
                checks=Check(
                    DataQualityChecks.is_numeric_or_null,
                    element_wise=True,
                ),
                nullable=True,
            )
        }
        )

        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df)
    
    def test_is_yyyy_mm_dd_valid_format_passes_schema(self):
        ''' Test that valid yyyy-mm-dd formatted dates pass the schema validation '''
        df = pd.DataFrame(
            {"period": ["2023-01-01", "1999-12-31"]}
        )

        schema = DataFrameSchema(
        {
            "period": Column(
                pa.String,
                checks=Check(
                    DataQualityChecks.is_yyyy_mm_dd,
                    element_wise=True,
                ),
                nullable=False,
            )
        }
        )

        schema.validate(df)
    
    def test_is_yyyy_mm_dd_invalid_format_fails_schema(self):
        ''' Test that invalid yyyy-mm-dd formatted dates fail the schema validation '''
        df = pd.DataFrame(
            {"period": ["2023-1-1", "2023/01/01", "23-01-01", None]}
        )

        schema = DataFrameSchema(
        {
            "period": Column(
                pa.String,
                checks=Check(
                    DataQualityChecks.is_yyyy_mm_dd,
                    element_wise=True,
                ),
                nullable=False,
            )
        }
        )

        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df)
    
    def test_check_is_string_passes_for_string(self):
        ''' Test that check_is_string passes for string values '''
        df = pd.DataFrame(
            {"text": ["hello", "", "123"]}
        )

        schema = DataFrameSchema(
        {
            "text": Column(
                pa.Object,
                checks=Check(
                    DataQualityChecks.check_is_string,
                    element_wise=True,
                ),
                nullable=False,
            )
        }
        )

        schema.validate(df)
    
    def test_check_is_string_fails_for_non_strings(self):
        ''' Test that check_is_string fails for non-string values '''
        df = pd.DataFrame(
            {"text": ["valid", 123, None]}
        )

        schema = DataFrameSchema(
        {
            "text": Column(
                pa.Object,
                checks=Check(
                    DataQualityChecks.check_is_string,
                    element_wise=True,
                ),
                nullable=False,
            )
        }
        )

        with pytest.raises(pa.errors.SchemaError):
            schema.validate(df)

    

    

    
