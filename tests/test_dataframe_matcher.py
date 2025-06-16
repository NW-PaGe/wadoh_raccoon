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
    def match_df(self):
        """Load the submission test DataFrame from disk."""
        submission_path = TEST_DATA_DIR / "match_df.parquet"
        return pl.read_parquet(submission_path)

    @pytest.fixture
    def match_to_df(self):
        """Load the WDRS test DataFrame from disk."""
        wdrs_path = TEST_DATA_DIR / "match_to_df.parquet"
        return pl.read_parquet(wdrs_path)
    
    # @pytest.fixture
    # def expected_exact_match_matched_df(self):
    #     """Load the expected exact_match() output from disk."""
    #     exact_match_matched_path = TEST_DATA_DIR / "exact_match_results_df.parquet"
    #     return pl.read_parquet(exact_match_matched_path)
    
    # @pytest.fixture
    # def expected_exact_match_unmatched_df(self):
    #     """Load the expected exact_match() output from disk."""
    #     exact_match_unmatched_path = TEST_DATA_DIR / "exact_unmatched_results_df.parquet"
    #     return pl.read_parquet(exact_match_unmatched_path)
    
    @pytest.fixture
    def expected_fuzzy_match_matched_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_match_matched_path = TEST_DATA_DIR / "fuzzy_match_results_df.parquet"
        return pl.read_parquet(fuzzy_match_matched_path)
    
    @pytest.fixture
    def expected_fuzzy_match_unmatched_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_match_unmatched_path = TEST_DATA_DIR / "fuzzy_unmatched_results_df.parquet"
        return pl.read_parquet(fuzzy_match_unmatched_path)
    
    # @pytest.fixture
    # def expected_match_matched_df(self):
    #     """Load the expected fuzzy_match() output from disk."""
    #     match_matched_path = TEST_DATA_DIR / "match_matched_results_df.parquet"
    #     return pl.read_parquet(match_matched_path)
    
    # @pytest.fixture
    # def expected_match_unmatched_df(self):
    #     """Load the expected fuzzy_match() output from disk."""
    #     match_unmatched_path = TEST_DATA_DIR / "match_unmatched_results_df.parquet"
    #     return pl.read_parquet(match_unmatched_path)

    def test_init(self, match_df, match_to_df):
        """Test that the DataFrameMatcher initializes correctly."""
        matcher = DataFrameMatcher(match_df, match_to_df)
        
        # Check that dataframes are stored correctly
        assert matcher.df_subm.equals(match_df)

        # Check that WDRS dataframe has the expected columns
        expected_columns = ["CASE_ID", "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                           "FILLER__ORDER__NUM", "FIRST_NAME", "LAST_NAME", 
                           "PATIENT_DOB", "SPECIMEN__COLLECTION__DTTM"]
        
        for col in expected_columns:
            assert col in matcher.df_wdrs.columns
        
        # # Check exact_matched is None initially
        # assert matcher.exact_matched is None
        # # Check exact_unmatched is None initially
        # assert matcher.exact_unmatched is None

    # def test_exact_match(self, match_df, match_to_df, expected_exact_match_matched_df, expected_exact_match_unmatched_df):
    #     """Test exact matching by Key field."""
    #     matcher = DataFrameMatcher(match_df, match_to_df)
    #     exact_matched_df, exact_unmatched_df = matcher.exact_match()

    #     # Use the correct method to compare Polars DataFrames
    #     assert exact_matched_df.equals(expected_exact_match_matched_df)
    #     assert exact_unmatched_df.equals(expected_exact_match_unmatched_df)

        
    def test_fuzzy_match(self, match_df, match_to_df, expected_fuzzy_match_matched_df, expected_fuzzy_match_unmatched_df):
        """Test fuzzy matching based on patient demographics."""
        matcher = DataFrameMatcher(match_df, match_to_df)
        exact_matched_df, exact_unmatched_df = matcher.exact_match()
        fuzzy_matched_df, fuzzy_unmatched_df = matcher.fuzzy_match()

        # Use the correct method to compare Polars DataFrames
        assert fuzzy_matched_df.equals(expected_fuzzy_match_matched_df)
        assert fuzzy_unmatched_df.equals(expected_fuzzy_match_unmatched_df)


    # def test_match_full_pipeline(self, match_df, match_to_df, expected_match_matched_df, expected_match_unmatched_df):
    #     """Test the complete matching pipeline combining exact and fuzzy matching."""
    #     matcher = DataFrameMatcher(match_df, match_to_df)
    #     matched, unmatched = matcher.match()
        
    #     # Use the correct method to compare Polars DataFrames
    #     assert matched.equals(expected_match_matched_df)
    #     assert unmatched.equals(expected_match_unmatched_df)
