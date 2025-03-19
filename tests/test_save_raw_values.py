import pytest
import polars as pl
from datetime import date
from wadoh_raccoon.utils import helpers  # Update this with the actual import path

def test_save_raw_values():
    # Sample input DataFrame
    data = pl.DataFrame({
        "lab_name": ["PHL", "MFT", "ELR", "PHL"],
        "first_name": ["Alice", "Bob", "Charlie", "Charlie"],
        "last_name": ["Smith", "Johnson", "Williams", "Williams"],
        "WA_ID": [1, 2, 4, 4]
    })

    # Call the function
    result_df = helpers.save_raw_values(df_inp=data, primary_key_col="WA_ID")

    # Check that the "raw_inbound_submission" column is a struct
    assert (pl.Struct in result_df.dtypes), "The 'raw_inbound_submission' column should be a struct"

    # Check that the output DataFrame has the expected columns
    expected_columns = ["submission_number", "internal_create_date", "raw_inbound_submission"]
    assert result_df.columns == expected_columns, f"Expected columns {expected_columns}, but got {result_df.columns}"
