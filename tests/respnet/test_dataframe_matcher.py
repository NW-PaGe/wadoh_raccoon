import polars as pl
from wadoh_raccoon.utils import helpers
from wadoh_raccoon import dataframe_matcher
import pytest

@pytest.fixture
def get_data():

    phl = pl.read_csv("tests/respnet/phl_test_data.csv")

    received_submissions_df = helpers.save_raw_values(phl,'PHLAccessionNumber')

    base_cols = ["submission_number", "internal_create_date"] + phl.columns 

    phl_df = (
        received_submissions_df
        .unnest(pl.col('raw_inbound_submission'))
        .select(sorted(base_cols))
    )

    wdrs = pl.read_csv("tests/respnet/phl_test_data_reference.csv")

    input_subm = phl_df.select([
        'PatientFirstName',
        'PatientLastName',
        'PatientBirthDate',
        'SpecimenDateCollected',
        'submission_number',
        'internal_create_date'])

    instance = dataframe_matcher.DataFrameMatcher(
        df_src=input_subm,
        df_ref=wdrs,
        first_name=('PatientFirstName', 'FIRST_NAME'),
        last_name=('PatientLastName', 'LAST_NAME'),
        dob=('PatientBirthDate', 'DOB'),
        spec_col_date=('SpecimenDateCollected', 'SPECIMEN_COLLECTION_DATE'),
        key='submission_number',
        threshold=80
    )

    result = instance.match()

    return result

def test_matched(get_data):

    result = get_data

    assert result.fuzzy_matched.height == 4


def test_unmatched(get_data):

    result = get_data

    assert result.fuzzy_unmatched.height == 4

def test_exact_match(get_data):

    result = get_data

    assert result.exact_matched.height == 2

def test_no_demo(get_data):

    result = get_data

    assert result.no_demo.height == 1