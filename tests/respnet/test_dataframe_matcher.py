import polars as pl
from wadoh_raccoon.utils import helpers
from wadoh_raccoon import dataframe_matcher
import pytest
import itertools
from pathlib import Path


TEST_DATA_DIR = Path(__file__).parent / "data"

@pytest.fixture
def phl():

    phl = pl.read_csv(TEST_DATA_DIR / "phl_test_data.csv")

    received_submissions_df = helpers.save_raw_values(phl,'PHLAccessionNumber')

    return (
        received_submissions_df
        .unnest(pl.col('raw_inbound_submission'))
        .select([
            'PatientFirstName',
            'PatientLastName',
            'PatientBirthDate',
            'SpecimenDateCollected',
            'submission_number',
            'internal_create_date'
        ])
    )

@pytest.fixture
def wdrs():
    return pl.read_csv(TEST_DATA_DIR / "phl_test_data_reference.csv")

def matcher(phl, wdrs, lazy, day_max, business_day_max):

    if lazy == 'lazy':
        phl = phl.lazy()
        wdrs = wdrs.lazy()

    instance = dataframe_matcher.DataFrameMatcher(
        df_src=phl,
        df_ref=wdrs,
        first_name=('PatientFirstName', 'FIRST_NAME'),
        last_name=('PatientLastName', 'LAST_NAME'),
        dob=('PatientBirthDate', 'DOB'),
        spec_col_date=('SpecimenDateCollected', 'SPECIMEN_COLLECTION_DATE'),
        key='submission_number',
        threshold=80,
        day_max=day_max,
        business_day_max=business_day_max
    )

    result = instance.match()

    return result, day_max, business_day_max

lazy_vals = ['lazy', 'eager']
day_vals = [None, 1, 4]
@pytest.fixture(params=list(itertools.product(lazy_vals, day_vals, day_vals)))
def result(phl, wdrs, request):
    lazy, day_max, business_day_max = request.param
    return matcher(phl, wdrs, lazy, day_max, business_day_max)


def test_matched(result):
    output, day_max, business_day_max = result
    if day_max is None and business_day_max is None:
        expected_height = 4
    elif day_max in {None, 4} and business_day_max in {None, 4}:
        expected_height = 3
    else:
        expected_height = 2
    assert helpers.lazy_height(output.fuzzy_matched) == expected_height

def test_unmatched(result):
    output, day_max, business_day_max = result
    if day_max is None and business_day_max is None:
        expected_height = 4
    elif day_max in {None, 4} and business_day_max in {None, 4}:
        expected_height = 5
    else:
        expected_height = 6
    assert helpers.lazy_height(output.fuzzy_unmatched) == expected_height

def test_exact_match(result):
    output, day_max, business_day_max = result
    assert helpers.lazy_height(output.exact_matched) == 2

def test_no_demo(result):
    output, day_max, business_day_max = result
    assert helpers.lazy_height(output.no_demo) == 1
