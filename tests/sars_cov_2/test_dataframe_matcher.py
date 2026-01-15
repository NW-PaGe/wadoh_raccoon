import pytest
import polars as pl
from polars.testing import assert_frame_equal
from pathlib import Path
import itertools

# Import the DataFrameMatcher class
from wadoh_raccoon.dataframe_matcher import DataFrameMatcher

# Path to test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"

class TestDataFrameMatcher:
    """Test suite for the DataFrameMatcher class."""

    @pytest.fixture
    def fuzzy_match_test_df(self):
        """Load the submission test DataFrame from disk."""
        fuzzy_match_test_df_path = TEST_DATA_DIR / "fuzzy_match_test_df.parquet"
        return pl.read_parquet(fuzzy_match_test_df_path)
        # fuzzy_match_test_df =  pl.read_parquet(fuzzy_match_test_df_path)

    @pytest.fixture
    def match_to_test_df(self):
        """Load the WDRS test DataFrame from disk."""
        match_to_test_df_path = TEST_DATA_DIR / "match_to_test_df.parquet"
        return pl.read_parquet(match_to_test_df_path)
        # match_to_test_df = pl.read_parquet(match_to_test_df_path)

    @pytest.fixture
    def exact_matched_test_exp_results_df(self):
        """Load the expected exact match output from disk."""
        exact_matched_test_exp_results_df_path = TEST_DATA_DIR / "exact_matched_test_exp_results_df.parquet"
        return pl.read_parquet(exact_matched_test_exp_results_df_path)

    @pytest.fixture
    def fuzzy_matched_test_exp_results_df(self):
        """Load the expected fuzzy match output from disk."""
        fuzzy_matched_test_exp_results_df_path = TEST_DATA_DIR / "fuzzy_matched_test_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_matched_test_exp_results_df_path)

    @pytest.fixture
    def fuzzy_matched_test_exp_results_daymaxed_df(self):
        """Load the expected fuzzy match output from disk."""
        fuzzy_matched_test_exp_results_daymaxed_df_path = TEST_DATA_DIR / "fuzzy_matched_test_exp_results_daymaxed_df.parquet"
        return pl.read_parquet(fuzzy_matched_test_exp_results_daymaxed_df_path)

    @pytest.fixture
    def fuzzy_unmatched_test_exp_results_df(self):
        """Load the expected fuzzy unmatch output from disk."""
        fuzzy_unmatched_test_exp_results_df_path = TEST_DATA_DIR / "fuzzy_unmatched_test_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_unmatched_test_exp_results_df_path)

    @pytest.fixture
    def fuzzy_unmatched_test_exp_results_daymaxed_df(self):
        """Load the expected fuzzy unmatch output from disk."""
        fuzzy_unmatched_test_exp_results_daymaxed_df_path = TEST_DATA_DIR / "fuzzy_unmatched_test_exp_results_daymaxed_df.parquet"
        return pl.read_parquet(fuzzy_unmatched_test_exp_results_daymaxed_df_path)

    @pytest.fixture
    def no_demo_test_exp_results_df(self):
        """Load the expected no demo output from disk."""
        no_demo_test_exp_results_df_path = TEST_DATA_DIR / "no_demo_test_exp_results_df.parquet"
        return pl.read_parquet(no_demo_test_exp_results_df_path)

    # Test DataFrames and LazyFrames
    @pytest.mark.parametrize('lazy', ['lazy', 'eager'])
    def test_init(self, fuzzy_match_test_df, match_to_test_df, lazy):
        """Test that the DataFrameMatcher initializes correctly."""

        if lazy == 'lazy':
            fuzzy_match_test_df = fuzzy_match_test_df.lazy()
            match_to_test_df = match_to_test_df.lazy()

        matcher = DataFrameMatcher(
            df_src=fuzzy_match_test_df,
            df_ref=match_to_test_df,
            first_name='FIRST_NAME',
            last_name='LAST_NAME',
            dob=('DOB', 'PATIENT_DOB'),
            spec_col_date=('SEQUENCE_SPECIMEN_COLLECTION_DATE', 'SPECIMEN__COLLECTION__DTTM'),
            key='submission_number'
        )
        
        # Check that dataframes are stored correctly
        # NOTE: this will not pass if key is not specified (a __key__ col will be created)
        assert_frame_equal(matcher.df_src, fuzzy_match_test_df)

        # Check that the match_to_test_df dataframe has the expected columns
        expected_columns = ["CASE_ID", "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                           "FILLER__ORDER__NUM", "FIRST_NAME", "LAST_NAME", 
                           "PATIENT_DOB", "SPECIMEN__COLLECTION__DTTM"]
        
        for col in expected_columns:
            assert col in matcher.df_ref.columns

    lazy_vals = ['lazy', 'eager']
    day_vals = [None, 1, 4]
    @pytest.mark.parametrize(
        ('lazy', 'day_max', 'business_day_max'),
        list(itertools.product(lazy_vals, day_vals, day_vals))
    )
    def test_fuzzy_match(self, 
                         fuzzy_match_test_df,
                         match_to_test_df,
                         exact_matched_test_exp_results_df,
                         fuzzy_matched_test_exp_results_df,
                         fuzzy_matched_test_exp_results_daymaxed_df,
                         fuzzy_unmatched_test_exp_results_df,
                         fuzzy_unmatched_test_exp_results_daymaxed_df,
                         no_demo_test_exp_results_df,
                         lazy,
                         day_max,
                         business_day_max):
        """Test fuzzy matching based on patient demographics."""

        if lazy == 'lazy':
            fuzzy_match_test_df = fuzzy_match_test_df.lazy()
            match_to_test_df = match_to_test_df.lazy()
            exact_matched_test_exp_results_df = exact_matched_test_exp_results_df.lazy()
            fuzzy_matched_test_exp_results_daymaxed_df = fuzzy_matched_test_exp_results_daymaxed_df.lazy()
            fuzzy_unmatched_test_exp_results_daymaxed_df = fuzzy_unmatched_test_exp_results_daymaxed_df.lazy()
            fuzzy_matched_test_exp_results_df = fuzzy_matched_test_exp_results_df.lazy()
            fuzzy_unmatched_test_exp_results_df = fuzzy_unmatched_test_exp_results_df.lazy()
            no_demo_test_exp_results_df = no_demo_test_exp_results_df.lazy()

        matcher = DataFrameMatcher(
            df_src=fuzzy_match_test_df,
            df_ref=match_to_test_df,
            first_name='FIRST_NAME',
            last_name='LAST_NAME',
            dob=('DOB', 'PATIENT_DOB'),
            spec_col_date=('SEQUENCE_SPECIMEN_COLLECTION_DATE', 'SPECIMEN__COLLECTION__DTTM'),
            key='submission_number',
            day_max=day_max,
            business_day_max=business_day_max
        )

        output = matcher.match()

        # Compare Polars DataFrames with expected results
        assert_frame_equal(output.exact_matched, exact_matched_test_exp_results_df)
        assert_frame_equal(output.no_demo, no_demo_test_exp_results_df)
        if day_max in {None, 4} and business_day_max in {None, 4}:
            assert_frame_equal(output.fuzzy_matched, fuzzy_matched_test_exp_results_df)
            assert_frame_equal(output.fuzzy_unmatched, fuzzy_unmatched_test_exp_results_df)
        else:
            assert_frame_equal(output.fuzzy_matched, fuzzy_matched_test_exp_results_daymaxed_df)
            assert_frame_equal(output.fuzzy_unmatched, fuzzy_unmatched_test_exp_results_daymaxed_df)
