import polars as pl
from thefuzz import fuzz
from wadoh_raccoon.utils import helpers

def clean_names(first_name: str, last_name: str):
    # ------- Process the Entire Table ------- #

    # mutate the entire table
    # remove null dates - for some reason couldn't do it in one step above
    return (
        pl.col(first_name).str.replace_all('[^a-zA-Z]','').str.to_uppercase().alias('first_name_clean'),
        pl.col(last_name).str.replace_all('[^a-zA-Z]','').str.to_uppercase().alias('last_name_clean')
    )

def prep_df(df, first_name, last_name, spec_col_date, dob, output_spec_col_name, output_dob_name):

    clean_df = (
        df
        .with_columns(
            # clean_names converts the names to first_name_clean & last_name_clean fyi
            clean_names(first_name=first_name, last_name=last_name),
            temp_spec_col=helpers.date_format(df=df, col=spec_col_date),
            temp_dob_col=helpers.date_format(df=df, col=dob)
        )
        .rename({"temp_spec_col": output_spec_col_name, "temp_dob_col": output_dob_name})
        # now drop the original name columns to clean up the namespace
        .select(pl.exclude([first_name, last_name, spec_col_date, dob]))
    )

    return clean_df

def filter_demo(submissions_to_fuzzy_prep):

    # 2. Split by presence of demographics and specimen collection date
    fuzzy_with_demo = (
        submissions_to_fuzzy_prep
        .filter(pl.col('first_name_clean').is_not_null() & 
                pl.col('last_name_clean').is_not_null() & 
                pl.col('submitted_collection_date').is_not_null() &
                pl.col('submitted_dob').is_not_null())
    )

    # save fuzzy without demographics
    fuzzy_without_demo = (
        submissions_to_fuzzy_prep
        .filter(pl.col('first_name_clean').is_null() |
                pl.col('last_name_clean').is_null() | 
                pl.col('submitted_collection_date').is_null() |
                pl.col('submitted_dob').is_null())
    )

    return fuzzy_with_demo, fuzzy_without_demo

def find_exact_match(ref, fuzzy_with_demo):

    potential_matches = (
        fuzzy_with_demo
        .join(ref,
            left_on=['first_name_clean','last_name_clean','submitted_dob'],
            right_on=['first_name_clean','last_name_clean','reference_dob'],
            how="left")
        .with_columns(
            date_subtract = (pl.col('submitted_collection_date') - pl.col('reference_collection_date'))
        )
        # for ones with multiple matches, pull the closest match based on collection date
        # .group_by('submission_number')
        # .agg([pl.all().sort_by('date_subtract').first()])
        .sort(by=['submission_number','date_subtract'],nulls_last=True)
        .unique(subset='submission_number',keep='first')
    )

    exact_match = potential_matches.filter((pl.col('CASE_ID').is_not_null()))

    needs_fuzzy_match = (
        potential_matches
        .filter(
            (pl.col('CASE_ID').is_null())
        )
        .select([
            'submission_number',
            'internal_create_date',
            'submitted_dob',
            'submitted_collection_date',
            # 'reference_collection_date', # this will get brought in during the dob_match join to ref df
            'first_name_clean',
            'last_name_clean',
        ])
    )

    # block/join based on dob
    # for the remaining records that need to be fuzzy matched, 
    # find all the records in the reference df that match based on dob
    # this will give us a smaller pool to actually fuzzy match the names against,
    # as opposed to fuzzy matching one name vs thousands
    dob_match = (
        needs_fuzzy_match
        .join(
            ref,
            left_on = 'submitted_dob',
            right_on='reference_dob',
            how = 'left'
        )
    )

    return exact_match, dob_match


class DataFrameMatcher:
    """
    A utility class for matching records.

    This class provides functionality to match submissions to cases (epi 
    data) based on exact matching via accessions or fuzzy matching based on
    patient demographics.
    """

    def __init__(
        self, 
        df_subm: pl.DataFrame, 
        df_ref: pl.DataFrame,

        first_name_ref: str,
        last_name_ref: str,
        dob_ref: str,
        spec_col_date_ref: str,

        last_name_src: str,
        first_name_src: str,
        dob_src: str,
        spec_col_date_src: str,

        key: str
        ):
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

        self.df_ref = df_ref
        self.df_subm = df_subm
        # self.df_to_process_inp = df_to_process_inp

        self.first_name_ref = first_name_ref
        self.last_name_ref = last_name_ref
        self.dob_ref = dob_ref
        self.spec_col_date_ref = spec_col_date_ref

        self.last_name_src = last_name_src
        self.first_name_src = first_name_src
        self.dob_src = dob_src
        self.spec_col_date_src = spec_col_date_src

        # submission key?
        self.key = key

    # TODO: Exact matching goes here?
    # clean dataframes first, then exact match

    def clean_all(self) -> pl.DataFrame:

        ref_prep = (
            prep_df(
                df=self.df_ref,
                first_name=self.first_name_ref,
                last_name=self.last_name_ref,
                spec_col_date=self.spec_col_date_ref,
                dob=self.dob_ref,
                output_spec_col_name='reference_collection_date',
                output_dob_name='reference_dob'
        )
        # Remove bad records
        .filter(
            (pl.col('first_name_clean').is_not_null()) &
            (pl.col('last_name_clean').is_not_null())
        )
    )

        submissions_to_fuzzy_prep = (
            prep_df(
                df=self.df_subm,
                first_name=self.first_name_src,
                last_name=self.last_name_src,
                spec_col_date=self.spec_col_date_src,
                dob=self.dob_src,
                output_spec_col_name='submitted_collection_date',
                output_dob_name='submitted_dob'
            )
        )


        return ref_prep, submissions_to_fuzzy_prep

    def filter_demo(self, submissions_to_fuzzy_prep):

        # 2. Split by presence of demographics and specimen collection date
        fuzzy_with_demo = (
            submissions_to_fuzzy_prep
            .filter(pl.col('first_name_clean').is_not_null() & 
                    pl.col('last_name_clean').is_not_null() & 
                    pl.col('submitted_collection_date').is_not_null() &
                    pl.col('submitted_dob').is_not_null())
        )

        # save fuzzy without demographics
        fuzzy_without_demo = (
            submissions_to_fuzzy_prep
            .filter(pl.col('first_name_clean').is_null() |
                    pl.col('last_name_clean').is_null() | 
                    pl.col('submitted_collection_date').is_null() |
                    pl.col('submitted_dob').is_null())
        )

        return fuzzy_with_demo, fuzzy_without_demo


    def find_exact_match(self, ref_prep, fuzzy_with_demo):

        potential_matches = (
            fuzzy_with_demo
            .join(ref_prep,
                left_on=['first_name_clean','last_name_clean','submitted_dob'],
                right_on=['first_name_clean','last_name_clean','reference_dob'],
                how="left",
                suffix="_em"
            )
            .with_columns(
                date_subtract = (pl.col('submitted_collection_date') - pl.col('reference_collection_date'))
            )
            # for ones with multiple matches, pull the closest match based on collection date
            # .group_by('submission_number')
            # .agg([pl.all().sort_by('date_subtract').first()])
            .sort(by=['submission_number','date_subtract'],nulls_last=True)
            .unique(subset='submission_number',keep='first')
        )

        exact_match = potential_matches.filter((pl.col('CASE_ID').is_not_null()))

        needs_fuzzy_match = (
            potential_matches
            .filter(
                (pl.col('CASE_ID').is_null())
            )
            # .select([
            #     # 'submission_number',
            #     # 'internal_create_date',
            #     'submitted_dob',
            #     'submitted_collection_date',
            #     # 'reference_collection_date', # this will get brought in during the dob_match join to ref df
            #     'first_name_clean',
            #     'last_name_clean',
            # ])
        )

        # block/join based on dob
        # for the remaining records that need to be fuzzy matched, 
        # find all the records in the reference df that match based on dob
        # this will give us a smaller pool to actually fuzzy match the names against,
        # as opposed to fuzzy matching one name vs thousands
        dob_match = (
            needs_fuzzy_match
            .join(
                ref_prep,
                left_on = 'submitted_dob',
                right_on='reference_dob',
                how = 'left'
            )
        )

        return exact_match, dob_match


    def fuzzy_match(self, dob_match, ref) -> pl.DataFrame:
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
        cross_join = ref.join(
            dob_match,
            how="cross",
            suffix="_cross_join"
        )

        # Add comparison columns with improved null handling
        compared = cross_join.with_columns([
            # Improved name comparison with explicit null handling
            pl.struct(["first_name_clean", "first_name_clean_right"]).map_elements(
                lambda x: fuzz.ratio(str(x["first_name_clean"].lower()), str(x["first_name_clean_right"].lower())) 
                if x["first_name_clean"] is not None and x["first_name_clean_right"] is not None 
                else 0,
                skip_nulls=False,
                return_dtype=pl.Int64
            ).alias("first_name_score"),
            
            pl.struct(["last_name_clean", "last_name_clean_right"]).map_elements(
                lambda x: fuzz.ratio(str(x["last_name_clean"].lower()), str(x["last_name_clean_right"].lower()))
                if x["last_name_clean"] is not None and x["last_name_clean_right"] is not None 
                else 0,
                skip_nulls=False,
                return_dtype=pl.Int64
            ).alias("last_name_score"),
            
            # Improved date comparison with coalesce
            pl.coalesce([
                (pl.col('submitted_dob').cast(pl.Utf8).str.slice(0, 10) == 
                pl.col("submitted_dob").cast(pl.Utf8).str.slice(0, 10))
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
            ((pl.col("submitted_collection_date").dt.date() - 
              pl.col("reference_collection_date"))
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
            self.key
        ).agg([
            pl.all().sort_by(["match_score", "collection_difference"], 
                             descending=[True, False]).first()
        ])

        match_attempts = match_attempts.sort(self.key, descending=False)

        # Filter into two dataframes; one fuzzy matched w/ a high degree of 
        # confidence and one fuzzy matched w/ a low degree of confidence (or
        # unmatched)
        fuzzy_matched = match_attempts.filter(pl.col("match_score") >= 80)
        fuzzy_unmatched = match_attempts.filter(
            (pl.col("match_score") < 80)
        )
        
        return fuzzy_matched, fuzzy_unmatched
    
    # TODO: Full matching goes here?

    def fuzzZ(self, verbose=True):
        
        # Process the Submissions to Fuzzy
        ref_prep, submissions_to_fuzzy_prep = self.clean_all()
        # Split by presence of demographics and specimen collection date
        fuzzy_with_demo, fuzzy_without_demo = self.filter_demo(submissions_to_fuzzy_prep)
        # find exact matches
        exact_match, dob_match = self.find_exact_match(ref_prep, fuzzy_with_demo)
        # find fuzzy matches
        fuzzy_matched, fuzzy_unmatched = self.fuzzy_match(exact_match, dob_match)
        # # print summary
        # if verbose:
        #     self.__output_summary(submissions_to_fuzzy_df, fuzzy_matched_review, fuzzy_without_demo, fuzzy_matched_none,
        #                fuzzy_matched_roster)
        # return fuzzy outputs
        return fuzzy_matched, fuzzy_unmatched, exact_match, fuzzy_without_demo