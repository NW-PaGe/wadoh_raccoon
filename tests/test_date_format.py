import polars as pl
from polars.testing import assert_frame_equal as pl_assert_frame_equal
import pytest
from wadoh_raccoon.utils import helpers

@pytest.fixture
def get_df():
    """
    Get the data
    """
    df = pl.DataFrame({'Name': [
                                 'Alice',
                                 'Bob', 
                                 'Aritra',
                                 'idk',
                                 'long_date',
                                #  'long_dateT',
                                 'monthday',
                                 'slashes',
                                 'longslash'],
                    #    'Age': [25, 30, 35, 3, 39],
                       'date': [
                                '2022-01-03',
                                '01-02-2020',
                                '44115',
                                None,
                                "2022-12-27 08:26:49",
                                # "2022-12-27T08:26:49",
                                '01/02/1995',
                                '2/3/2022',
                                '2/16/2022'
                                ]})
    df_output = (
        df
        .with_columns(
            output_date = helpers.date_format('date')
        )
        .select('output_date')
    )

    return df, df_output


# ---- test the function ---- #

# test with polars
def test_date_format_polars(get_df):
    """
    Test if the column names of the transformed dataframe
    match the columns of the expected outputs
    """
    _, df_output = get_df

    x = (
        pl.DataFrame({'output_date': [
            '2022-01-03',
            '2020-01-02',
            None,
            None,
            "2022-12-27",
            # "2022-12-27",
            '1995-01-02',
            '2022-02-03',
            '2022-02-16']})
        .with_columns(
            pl.col('output_date').cast(pl.Date)
        )
    )

    pl_assert_frame_equal(df_output,x)
