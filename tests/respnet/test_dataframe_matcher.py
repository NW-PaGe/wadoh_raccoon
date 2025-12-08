import polars as pl
from wadoh_raccoon.utils import helpers
from wadoh_raccoon import dataframe_matcher
import pytest

@pytest.fixture
def phl():

    phl = pl.read_csv("tests/respnet/phl_test_data.csv")

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
    return pl.read_csv("tests/respnet/phl_test_data_reference.csv")

def matcher(phl, wdrs, lazy):

    if lazy == 'lazy':
        phl = phl.lazy()
        wdrs = wdrs.lazy()

    instance = dataframe_matcher.DataFrameMatcher(
        df_subm=phl,
        df_ref=wdrs,

        first_name_ref='FIRST_NAME',
        last_name_ref='LAST_NAME',
        dob_ref='DOB',
        spec_col_date_ref='SPECIMEN_COLLECTION_DATE',

        first_name_src='PatientFirstName',
        last_name_src='PatientLastName',
        dob_src='PatientBirthDate',
        spec_col_date_src='SpecimenDateCollected',
        
        key='submission_number',
        threshold=80
    )

    # fuzz_matched, fuzz_unmatched = instance.fuzzy_match()

    result = instance.match()

    return result


@pytest.fixture(params=["lazy", "eager"])
def result(phl, wdrs, request):
    return matcher(phl, wdrs, request.param)


def test_matched(result):
    assert helpers.lazy_height(result.fuzzy_matched) == 4

def test_unmatched(result):
    assert helpers.lazy_height(result.fuzzy_unmatched) == 4

def test_exact_match(result):
    assert helpers.lazy_height(result.exact_matched) == 2

def test_no_demo(result):
    assert helpers.lazy_height(result.no_demo) == 1
