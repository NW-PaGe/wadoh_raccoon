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
            Submissions dataframe containing Key (The WA number or PHL 
            accession number assigned by PHL to isolates), patient demographics 
            pulled from LIMS based on WA number, and the submission data.
        
        df_wdrs (pl.DataFrame): 
            WDRS queried dataframe with CASE_ID, columns potentially containing 
            the key (i.e. SPECIMEN__ID__ACCESSION__NUM__MANUAL, 
            FILLER__ORDER__NUM), patient demographics, and accompanying case 
            data. 

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
        self.exact_matched = None
        self.exact_unmatched = None
        
    def exact_match(self) -> pl.DataFrame:
        """
        Perform exact matching to return CASE_ID (self.df_wdrs) by matching the 
        Key field (self.db_subm) to SPECIMEN__ID__ACCESSION__NUM__MANUAL and 
        FILLER__ORDER__NUM fields (self.df_wdrs).

        Parameters:
        -----------
        self : object
            Instance of class.

        Returns:
        --------
        pl.DataFrame
            subm_df records with matched CASE_ID, where found.
        """
        # Match on SPECIMEN__ID__ACCESSION__NUM__MANUAL
        df_with_accession = self.df_subm.join(
            self.df_wdrs,
            left_on="Key",
            right_on="SPECIMEN__ID__ACCESSION__NUM__MANUAL",
            how="left",
            coalesce=False
        )

        # Get bool of rows where CASE_ID is null (i.e. matching on 
        # SPECIMEN__ID__ACCESSION__NUM__MANUAL was unsuccessful) 
        unmatched_mask = df_with_accession["CASE_ID"].is_null()

        # Split into a matched df and a df that stil requires a match.
        match_on_accession_attempt = df_with_accession.filter(~unmatched_mask)
        to_match_order = df_with_accession.filter(
            unmatched_mask
            ).drop(["CASE_ID", 
                    "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                    "FILLER__ORDER__NUM",
                    "FIRST_NAME_right",
                    "LAST_NAME_right",
                    "PATIENT_DOB_right", 
                    "SPECIMEN__COLLECTION__DTTM_right"
                    ])


        # Match on FILLER__ORDER__NUM
        match_on_filler_attempt = to_match_order.join(
            self.df_wdrs,
            left_on="Key",
            right_on="FILLER__ORDER__NUM",
            how="left",
            coalesce=False
        )

        # Combine attempts of matching
        match_attempts = pl.concat([
            match_on_accession_attempt,
            match_on_filler_attempt
        ])

        # Sort
        match_attempts = match_attempts.sort("Key", descending=False)

        # Get bool of rows where CASE_ID is null (i.e. matching on 
        # SPECIMEN__ID__ACCESSION__NUM__MANUAL and FILLER__ORDER__NUM both 
        # unsuccessful) 
        unmatched_mask = match_attempts["CASE_ID"].is_null()

        # Return two dfs; matched and unmatched
        self.exact_matched = match_attempts.filter(~unmatched_mask)
        self.exact_unmatched = match_attempts.filter(unmatched_mask)
        return self.exact_matched, self.exact_unmatched


    def fuzzy_match(self, df_to_match: pl.DataFrame = None) -> pl.DataFrame:
        """
        Perform fuzzy matching to find CASE_ID by fuzzy matching on FIRST_NAME, 
        LAST_NAME, PATIENT_DOB, and SPECIMEN__COLLECTION__DTTM.
        
        Parameters:
        -----------
        self : object
            Instance of class

        df_to_match : pl.DataFrame, optional 
            DataFrame to match against df_wdrs. If None, uses self.exact_matches.

        Returns:
        --------
        pl.DataFrame
            subm_df records with matched CASE_ID, where found.
        """
        if df_to_match is None:
            to_fuzzy_match_df = self.exact_unmatched
        
        # Create a cross join to compare all records
        # Note: depending on the size of the datasets may be too resource intensive
        to_fuzzy_match = to_fuzzy_match_df.drop(["CASE_ID", 
                    "SPECIMEN__ID__ACCESSION__NUM__MANUAL", 
                    "FILLER__ORDER__NUM",
                    "FIRST_NAME_right",
                    "LAST_NAME_right",
                    "PATIENT_DOB_right", 
                    "SPECIMEN__COLLECTION__DTTM_right"
                    ])

        cross_join = to_fuzzy_match.join(
            self.df_wdrs,
            how="cross"
        )

        # Add comparison columns with improved null handling
        compared = cross_join.with_columns([
            # Improved name comparison with explicit null handling
            pl.struct(["FIRST_NAME", "FIRST_NAME_right"]).map_elements(
                lambda x: fuzz.ratio(str(x["FIRST_NAME"].lower()), str(x["FIRST_NAME_right"].lower())) 
                if x["FIRST_NAME"] is not None and x["FIRST_NAME_right"] is not None 
                else 0
            ).alias("first_name_score"),
            
            pl.struct(["LAST_NAME", "LAST_NAME_right"]).map_elements(
                lambda x: fuzz.ratio(str(x["LAST_NAME"].lower()), str(x["LAST_NAME_right"].lower()))
                if x["LAST_NAME"] is not None and x["LAST_NAME_right"] is not None 
                else 0
            ).alias("last_name_score"),
            
            # Improved date comparison with coalesce
            pl.coalesce([
                (pl.col("PATIENT_DOB").cast(pl.Utf8).str.slice(0, 10) == 
                pl.col("PATIENT_DOB_right").cast(pl.Utf8).str.slice(0, 10))
                .cast(pl.Int64).mul(100),
                pl.lit(0)
            ]).alias("dob_score"),
            
            pl.coalesce([
                (pl.col("SPECIMEN__COLLECTION__DTTM").cast(pl.Utf8).str.slice(0, 10) == 
                pl.col("SPECIMEN__COLLECTION__DTTM_right").cast(pl.Utf8).str.slice(0, 10))
                .cast(pl.Int64).mul(100),
                pl.lit(0)
            ]).alias("date_coll_score")
        ])

        # Calculate weighted score with validation
        scored = compared.with_columns([
            pl.coalesce([
                (
                    0.7 * ((pl.col("first_name_score") + pl.col("last_name_score")) / 2) +
                    0.15 * pl.col("dob_score") +
                    0.15 * pl.col("date_coll_score")
                ),
                pl.lit(0)  # Fallback if any component is null
            ]).alias("match_score")
        ])

        # Filter for matches above threshold and get best match per record
        match_attempts = scored.group_by(
            "Key"
        ).agg([
            pl.all().sort_by("match_score", descending=True).first()
        ])

        # Sort
        match_attempts = match_attempts.sort("Key", descending=False)

        # Filter into two dataframes; one fuzzy matched w/ a high degree of 
        # confidence and one fuzzy matched w/ a low degree of confidence (or
        # unmatched)
        fuzzy_matched = match_attempts.filter(pl.col("match_score") >= 80)
        fuzzy_unmatched = match_attempts.filter(
            (pl.col("match_score") < 80)
        )
        
        return fuzzy_matched, fuzzy_unmatched
    
    
    def match(self) -> pl.DataFrame:
        """
        Perform both exact and fuzzy matching in sequence.
        
        Parameters:
        -----------
        self: object
            Instance of class.
        
        Returns:
        --------
        pl.DataFrame
            Combined results from both matching methods.
        """
        # First perform exact matching
        self.exact_match()
        
        # Then perform fuzzy matching on unmatched records
        fuzzy_matched, fuzzy_unmatched = self.fuzzy_match()
        
        # Combination of all matched records from both exact_match() and fuzzy_match()
        all_matched = pl.concat([self.exact_matched,
                          fuzzy_matched],
                          how='align')
        
        # Note: fuzzy_unmatched represents any record(s) that could not be 
        # matched at all (i.e failed on exact_match() and fuzzy_match())

        return all_matched, fuzzy_unmatched
