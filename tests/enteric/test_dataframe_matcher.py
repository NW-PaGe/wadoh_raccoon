import pytest
import polars as pl
from pathlib import Path

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
    def fuzzy_matched_test_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_matched_test_exp_results_df_path = TEST_DATA_DIR / "fuzzy_matched_test_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_matched_test_exp_results_df_path)
        # fuzzy_matched_test_exp_results_df = pl.read_parquet(fuzzy_matched_test_exp_results_df_path)
    
    @pytest.fixture
    def fuzzy_unmatched_test_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_unmatched_test_exp_results_df_path = TEST_DATA_DIR / "fuzzy_unmatched_test_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_unmatched_test_exp_results_df_path)
        # fuzzy_unmatched_test_exp_results_df =  pl.read_parquet(fuzzy_unmatched_test_exp_results_df_path)


    def test_init(self, fuzzy_match_test_df, match_to_test_df):
        """Test that the DataFrameMatcher initializes correctly."""
        matcher = DataFrameMatcher(fuzzy_match_test_df, match_to_test_df)
        
        # Check that dataframes are stored correctly
        assert matcher.df_subm.equals(fuzzy_match_test_df)

        # Check that the match_to_test_df dataframe has the expected columns
        expected_columns = ["CASE_ID", "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                           "FILLER__ORDER__NUM", "FIRST_NAME", "LAST_NAME", 
                           "PATIENT_DOB", "SPECIMEN__COLLECTION__DTTM"]
        
        for col in expected_columns:
            assert col in matcher.df_wdrs.columns

        
    def test_fuzzy_match(self, 
                         fuzzy_match_test_df,
                         match_to_test_df,
                         fuzzy_matched_test_exp_results_df, 
                         fuzzy_unmatched_test_exp_results_df,):
        """Test fuzzy matching based on patient demographics."""
        matcher = DataFrameMatcher(fuzzy_match_test_df, match_to_test_df)
        fuzzy_matched_df, fuzzy_unmatched_df = matcher.fuzzy_match()

        # Use the correct method to compare Polars DataFrames
        assert fuzzy_matched_df.equals(fuzzy_matched_test_exp_results_df)
        assert fuzzy_unmatched_df.equals(fuzzy_unmatched_test_exp_results_df)
