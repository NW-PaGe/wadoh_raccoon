import polars as pl
import pytest
from wadoh_raccoon.utils import helpers


# ---- test the function ---- #

# test with polars

# Test empty and non-empty frames
@pytest.mark.parametrize("height", [0, 100])
# Test DataFrames and LazyFrames
@pytest.mark.parametrize('lazy', ['lazy', 'eager'])
def test_frame_height(height, lazy):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """

    df = pl.DataFrame({'column_1': range(height)})

    if lazy == 'lazy':
        df = df.lazy()

    output_height = helpers.lazy_height(df)

    assert output_height == height, f"Real DataFrame height is {height}, calculated height is {output_height}"
