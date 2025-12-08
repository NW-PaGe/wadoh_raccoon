import polars as pl
from polars.testing import assert_frame_equal as pl_assert_frame_equal
import pytest
from wadoh_raccoon.utils import helpers


@pytest.fixture
def input_df():
    """Get the input data"""
    return pl.DataFrame({
        'Name': [
            'Alice', 'Bob', 'Aritra', 'idk', 'long_date', 'monthday', 'slashes', 'longslash'
        ],
        'date': [
            '2022-01-03', '01-02-2020', '44115', None, '2022-12-27 08:26:49', '01/02/1995', '2/3/2022', '2/16/2022'
        ]
    })

@pytest.fixture
def output_df():
    """Get the output data"""
    return (
        pl.DataFrame({
            'output_date':
                ['2022-01-03', '2020-01-02', None, None, '2022-12-27', '1995-01-02', '2022-02-03', '2022-02-16']
        })
        .with_columns(pl.col('output_date').cast(pl.Date))
    )

@pytest.mark.parametrize('lazy', ['lazy', 'eager'])

# ---- test the function ---- #

# test with polars
def test_date_format_polars(input_df, output_df, lazy):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """

    if lazy == 'lazy':
        input_df = input_df.lazy()
        output_df = output_df.lazy()

    df = input_df.with_columns(output_date = helpers.date_format(df=input_df,col='date')).select('output_date')

    pl_assert_frame_equal(df, output_df)
