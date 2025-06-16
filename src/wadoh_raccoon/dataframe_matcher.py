import polars as pl
from thefuzz import fuzz

class DataFrameMatcher:
    """
    A utility class for matching records.

    This class provides functionality to match submissions to cases (epi 
    data) based on exact matching via accessions or fuzzy matching based on
    patient demographics.
    """

    def __init__(self, df_subm: pl.DataFrame, df_wdrs: pl.DataFrame):
        """
        Initialize the DataFrameMatcher with two dataframes.

        Parameters:
        -----------
        df_subm (pl.DataFrame): 
            Submissions dataframe containing Key(s), patient demographics (if
            available), and the submission data.
        
        df_wdrs (pl.DataFrame): 
            WDRS queried dataframe with CASE_ID, columns potentially containing 
            the key, patient demographics. 

        Returns:
        --------
        None
        """

        self.df_subm = df_subm
        self.df_wdrs = df_wdrs.select(["CASE_ID", 
                                       "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                                       "FILLER__ORDER__NUM",
                                       "FIRST_NAME",
                                       "LAST_NAME",
                                       "PATIENT_DOB", 
                                       "SPECIMEN__COLLECTION__DTTM"
                                       ])

    # TODO: Exact matching goes here?

    def fuzzy_match(self) -> pl.DataFrame:
        """
        Perform fuzzy matching to find CASE_ID by fuzzy matching on FIRST_NAME, 
        LAST_NAME, PATIENT_DOB, and SPECIMEN__COLLECTION__DTTM.
        
        Parameters:
        -----------
        self : object
            Instance of class

        Returns:
        --------
        pl.DataFrame
            subm_df records with matched CASE_ID, where found.
        """
        
        # Create a cross join to compare all records
        # Note: depending on the size of the datasets may be too resource intensive
        cross_join = self.df_subm.join(
            self.df_wdrs,
            how="cross"
        )

        # Add comparison columns with improved null handling
        compared = cross_join.with_columns([
            # Improved name comparison with explicit null handling
            pl.struct(["FIRST_NAME", "FIRST_NAME_right"]).map_elements(
                lambda x: fuzz.ratio(str(x["FIRST_NAME"].lower()), str(x["FIRST_NAME_right"].lower())) 
                if x["FIRST_NAME"] is not None and x["FIRST_NAME_right"] is not None 
                else 0,
                skip_nulls=False
            ).alias("first_name_score"),
            
            pl.struct(["LAST_NAME", "LAST_NAME_right"]).map_elements(
                lambda x: fuzz.ratio(str(x["LAST_NAME"].lower()), str(x["LAST_NAME_right"].lower()))
                if x["LAST_NAME"] is not None and x["LAST_NAME_right"] is not None 
                else 0,
                skip_nulls=False
            ).alias("last_name_score"),
            
            # Improved date comparison with coalesce
            pl.coalesce([
                (pl.col("PATIENT_DOB").cast(pl.Utf8).str.slice(0, 10) == 
                pl.col("PATIENT_DOB_right").cast(pl.Utf8).str.slice(0, 10))
                .cast(pl.Int64).mul(100),
                pl.lit(0)
            ]).alias("dob_score"),
        ])

        # Calculate weighted score with validation
        scored = compared.with_columns([
            pl.coalesce([
                (
                    0.8 * ((pl.col("first_name_score") + pl.col("last_name_score")) / 2) +
                    0.2 * pl.col("dob_score")
                ),
                pl.lit(0)  # Fallback if any component is null
            ]).alias("match_score")
        ])

        scored_diff = scored.with_columns(
            ((pl.col("SPECIMEN__COLLECTION__DTTM").dt.date() - 
              pl.col("SPECIMEN__COLLECTION__DTTM_right"))
              .abs().alias("collection_difference"))
        )

        # Filter for matches above threshold to get best match per record
        # Note: Filters by highest match score followed by the difference (days)
        # between the submitted collection date and the collection date in WDRS 
        # (lower is better). There can be multiple cases in WDRS tied to the 
        # same person. Ex. There are 3 different cases with the same 
        # match_score of 100 but the collection_difference is 12, 91, 200 days 
        # apart it will take the case that is 12 days apart between the 
        # collection dates
        match_attempts = scored_diff.group_by(
            "Key"
        ).agg([
            pl.all().sort_by(["match_score", "collection_difference"], 
                             descending=[True, False]).first()
        ])

        match_attempts = match_attempts.sort("Key", descending=False)

        # Filter into two dataframes; one fuzzy matched w/ a high degree of 
        # confidence and one fuzzy matched w/ a low degree of confidence (or
        # unmatched)
        fuzzy_matched = match_attempts.filter(pl.col("match_score") >= 80)
        fuzzy_unmatched = match_attempts.filter(
            (pl.col("match_score") < 80)
        )
        
        return fuzzy_matched, fuzzy_unmatched
    
    # TODO: Full matching goes here?