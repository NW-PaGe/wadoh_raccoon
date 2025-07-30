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

    @pytest.fixture
    def match_to_test_df(self):
        """Load the WDRS test DataFrame from disk."""
        match_to_test_df_path = TEST_DATA_DIR / "match_to_test_df.parquet"
        return pl.read_parquet(match_to_test_df_path)
    
    @pytest.fixture
    def exact_matched_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        exact_matched_exp_results_df_path = TEST_DATA_DIR / "exact_matched_exp_results_df.parquet"
        return pl.read_parquet(exact_matched_exp_results_df_path)
    
    @pytest.fixture
    def fuzzy_matched_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_matched_exp_results_df_path = TEST_DATA_DIR / "fuzzy_matched_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_matched_exp_results_df_path)
    
    @pytest.fixture
    def fuzzy_unmatched_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_unmatched_exp_results_df_path = TEST_DATA_DIR / "fuzzy_unmatched_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_unmatched_exp_results_df_path)
    
    @pytest.fixture
    def fuzzy_review_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_review_exp_results_df_path = TEST_DATA_DIR / "fuzzy_review_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_review_exp_results_df_path)
    
    @pytest.fixture
    def fuzzy_without_demo_exp_results_df(self):
        """Load the expected fuzzy_match() output from disk."""
        fuzzy_without_demo_exp_results_df_path = TEST_DATA_DIR / "fuzzy_without_demo_exp_results_df.parquet"
        return pl.read_parquet(fuzzy_without_demo_exp_results_df_path)

    def test_init(self, fuzzy_match_test_df, match_to_test_df):
        """Test that the DataFrameMatcher initializes correctly."""
        matcher = DataFrameMatcher(
            df_subm=fuzzy_match_test_df, 
            df_ref=match_to_test_df,
            first_name="FIRST_NAME",
            last_name="LAST_NAME", 
            dob="PATIENT_DOB",
            spec_col_date="SPECIMEN__COLLECTION__DTTM",
            key="Key"
        )
        
        # Check that dataframes are stored correctly
        assert matcher.df_subm.equals(fuzzy_match_test_df)
        assert matcher.df_ref.equals(match_to_test_df)

        # Check that column names are set correctly
        assert matcher.first_name_src == "FIRST_NAME"
        assert matcher.first_name_ref == "FIRST_NAME"
        assert matcher.last_name_src == "LAST_NAME"
        assert matcher.last_name_ref == "LAST_NAME"
        assert matcher.dob_src == "PATIENT_DOB"
        assert matcher.dob_ref == "PATIENT_DOB"
        assert matcher.spec_col_date_src == "SPECIMEN__COLLECTION__DTTM"
        assert matcher.spec_col_date_ref == "SPECIMEN__COLLECTION__DTTM"
        assert matcher.key == "Key"

        # Check that the match_to_test_df dataframe has the expected columns
        expected_columns = ["CASE_ID", "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                           "FILLER__ORDER__NUM", "FIRST_NAME", "LAST_NAME", 
                           "PATIENT_DOB", "SPECIMEN__COLLECTION__DTTM"]
        
        for col in expected_columns:
            assert col in matcher.df_ref.columns

    def test_fuzzy_match(self, 
                         fuzzy_match_test_df,
                         match_to_test_df,
                         fuzzy_matched_exp_results_df, 
                         fuzzy_unmatched_exp_results_df,
                         fuzzy_review_exp_results_df,
                         fuzzy_without_demo_exp_results_df):
        """Test fuzzy matching based on patient demographics."""
        matcher = DataFrameMatcher(
            df_subm=fuzzy_match_test_df, 
            df_ref=match_to_test_df,
            first_name="FIRST_NAME",
            last_name="LAST_NAME", 
            dob="PATIENT_DOB",
            spec_col_date="SPECIMEN__COLLECTION__DTTM",
            key="Key"
        )
        
        # Run the full fuzzy matching process
        fuzzy_matched, fuzzy_unmatched, exact_match, fuzzy_without_demo, fuzzy_review = matcher.fuzzZ(verbose=False)

        # Based on the method signature, fuzzy_match() returns fuzzy_review, fuzzy_unmatched, fuzzy_matched
        # But the test expects fuzzy_matched_df, fuzzy_unmatched_df
        # Assuming the test wants to compare the main matched and unmatched results
        # Use the correct method to compare Polars DataFrames
        assert fuzzy_matched.equals(fuzzy_matched_exp_results_df)
        assert fuzzy_unmatched.equals(fuzzy_unmatched_exp_results_df)
        assert fuzzy_review.equals(fuzzy_review_exp_results_df)
        assert fuzzy_without_demo.equals(fuzzy_without_demo_exp_results_df)
