import polars as pl
from thefuzz import fuzz
from wadoh_raccoon.utils import helpers


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
        threshold: int | float = 80,

        first_name: str | None = None,
        last_name: str | None = None,
        dob: str | None = None,
        spec_col_date: str | None = None,

        first_name_src: str | None = None,
        last_name_src: str | None = None,
        dob_src: str | None = None,
        spec_col_date_src: str | None = None,

        first_name_ref: str | None = None,
        last_name_ref: str | None = None,
        dob_ref: str | None = None,
        spec_col_date_ref: str | None = None,

        key: str | None = None
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

        # Check col name param sets
        self.__demo_param_checker(first_name, first_name_ref, first_name_src, "first_name")
        self.__demo_param_checker(last_name, last_name_ref, last_name_src, "last_name")
        self.__demo_param_checker(dob, dob_ref, dob_src, "dob")
        self.__demo_param_checker(spec_col_date, spec_col_date_ref, spec_col_date_src, "spec_col_date")

        # Source and reference data
        self.df_subm = df_subm
        self.df_ref = df_ref

        # Column names
        if first_name:
            self.first_name_src = str(first_name)
            self.first_name_ref = str(first_name)
        else:
            self.first_name_src = str(first_name_src)
            self.first_name_ref = str(first_name_ref)

        if last_name:
            self.last_name_src = str(last_name)
            self.last_name_ref = str(last_name)
        else:
            self.last_name_src = str(last_name_src)
            self.last_name_ref = str(last_name_ref)

        if dob:
            self.dob_src = str(dob)
            self.dob_ref = str(dob)
        else:
            self.dob_src = str(dob_src)
            self.dob_ref = str(dob_ref)

        if spec_col_date:
            self.spec_col_date_src = str(spec_col_date)
            self.spec_col_date_ref = str(spec_col_date)
        else:
            self.spec_col_date_src = str(spec_col_date_src)
            self.spec_col_date_ref = str(spec_col_date_ref)

        # submission key
        if key is None:
            self.key = '___key___'
            self.key_isnone = True
            self.df_subm = self.df_subm.with_row_index(name=self.key)
        else:
            self.key = key
            self.key_isnone = False

        # threshold
        self.threshold = threshold

    @staticmethod
    def __prep_df(df, first_name, last_name, spec_col_date, dob, output_spec_col_name, output_dob_name):

        clean_df = (
            df
            .with_columns(
                # clean_name converts the names to first_name_clean & last_name_clean
                helpers.clean_name(first_name).alias("first_name_clean"),
                helpers.clean_name(last_name).alias("last_name_clean"),
                temp_spec_col=helpers.date_format(df=df, col=spec_col_date),
                temp_dob_col=helpers.date_format(df=df, col=dob)
            )
            .rename({"temp_spec_col": output_spec_col_name, "temp_dob_col": output_dob_name})
        )

        return clean_df

    @staticmethod
    def __demo_param_checker(param, param_src, param_ref, param_name):
        if param is None and (param_src is None or param_ref is None):
            raise ValueError(f"`{param_name}` or both `{param_name}_src` and `{param_name}_ref` must not be None")

    def clean_all(self) -> (pl.DataFrame, pl.DataFrame):

        ref_prep = (
            self.__prep_df(
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
            self.__prep_df(
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

    @staticmethod
    def filter_demo(submissions_to_fuzzy_prep) -> (pl.DataFrame, pl.DataFrame):

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


    def find_exact_match(self, ref_prep, fuzzy_with_demo) -> (pl.DataFrame, pl.DataFrame):

        indicator = '___indicator___'  # Name for temp indicator col to determine join outcome

        potential_matches = (
            fuzzy_with_demo
            .join(ref_prep.with_columns(pl.lit(True).alias(indicator)),  # Add indicator column to determine join
                left_on=['first_name_clean','last_name_clean','submitted_dob'],
                right_on=['first_name_clean','last_name_clean','reference_dob'],
                how="left",
                suffix="_em"
            )
            .with_columns(
                date_subtract = (pl.col('submitted_collection_date') - pl.col('reference_collection_date')).abs()
            )
            # for ones with multiple matches, pull the closest match based on collection date
            # .group_by(self.key)
            # .agg([pl.all().sort_by('date_subtract').first()])
            .sort(by=[self.key,'date_subtract'], nulls_last=True)
            .unique(subset=self.key, keep='first')
        )

        exact_match = (
            potential_matches
            .filter(pl.col(indicator).is_not_null())  # Keep only fields with ref_prep joined
            .drop(indicator)  # Drop the temp indicator col
        )

        needs_fuzzy_match = (
            potential_matches
            .filter(pl.col(indicator).is_null())  # Keep only fields with ref_prep not joined
            .drop(indicator)  # Drop the temp indicator col
            .select(fuzzy_with_demo.collect_schema().names())  # Drop the previously joined null value cols
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

    def score(self, df):
        return (
            df
            .with_columns(

                # First get the fuzz ratio with the first name
                pl.struct(['first_name_clean', 'first_name_clean_right'])
                .map_elements(
                    lambda cols: fuzz.ratio(cols['first_name_clean'], cols['first_name_clean_right']),
                    skip_nulls=False,
                    return_dtype=pl.Int64
                )
                .alias('first_name_result'),

                # Now get the fuzz ratio with the last name
                pl.struct(['last_name_clean', 'last_name_clean_right'])
                .map_elements(
                    lambda cols: fuzz.ratio(cols['last_name_clean'], cols['last_name_clean_right']),
                    skip_nulls=False,
                    return_dtype=pl.Int64
                )
                .alias('last_name_result'),

                # Now reverse - WDRS is known to switch first and last names
                # First get the fuzz ratio with the first name
                pl.struct(['first_name_clean', 'last_name_clean_right'])
                .map_elements(
                    lambda cols: fuzz.ratio(cols['first_name_clean'], cols['last_name_clean_right']),
                    skip_nulls=False,
                    return_dtype=pl.Int64
                )
                .alias('reverse_first_name_result'),

                # Now get the fuzz ratio with the last name
                pl.struct(['last_name_clean', 'first_name_clean_right'])
                .map_elements(
                    lambda cols: fuzz.ratio(cols['last_name_clean'], cols['first_name_clean_right']),
                    skip_nulls=False,
                    return_dtype=pl.Int64
                )
                .alias('reverse_last_name_result'),

            )
            .with_columns(
                # Now get the ratios between first and last name matches
                pl.mean_horizontal('first_name_result', 'last_name_result').alias('match_ratio'),
                pl.mean_horizontal('reverse_first_name_result', 'reverse_last_name_result').alias(
                    'reverse_match_ratio')
            )
        )

    def fuzzy_match(self, dob_match) -> (pl.DataFrame, pl.DataFrame):
        """ 

        Where the magic happens. Do the fuzzy matching to the dataframe

        Parameters
        ----------
        dob_match: pl.DataFrame
            the dataframe that has records grouped by their dob match
        
        Returns
        ----------
        fuzzy_matched: pl.DataFrame
            dataframe with matches that met or exceeded the fuzzy matching score threshold
        fuzzy_unmatched: pl.DataFrame
            dataframe with matches that failed to meet the fuzzy matching score threshold
        
        Examples
        --------
        ```{python}
        from wadoh_racoon import dataframe_matcher as dfm
        from datetime import date

        # Create example data
        df = pl.DataFrame({
            'submission_number': [453278555, 453278555, 887730141],
            'first_name_clean': ['DAVIS', 'DAVIS', 'GRANT'],
            'last_name_clean': ['SMITHDAVIS', 'SMITHDAVIS', 'MITHCELL'],
            'submitted_collection_date': [date(2024, 11, 29), date(2024, 11, 29), date(2024, 12, 2)],
            'submitted_dob': [date(1989, 7, 15), date(1989, 7, 15), date(1990, 6, 21)],
             'CASE_ID': [100000032, 100000041, None],
            'first_name_clean_right': ['DAVID', 'DAVID', None],
            'last_name_clean_right': ['SMITDAVIS', 'SMITDAVIS', None],
             'reference_collection_date': [date(2024, 11, 29), date(2024, 8, 31), None]
        })

        # Init dataframe matcher
        # (this is not how to use the instance but input data not used in this example)
        instance = dfm.DataFrameMatcher(df, df)
        fuzzy_matched, fuzzy_unmatched = instance.fuzzy_match(dob_match=df)
        ```
        
        Fuzzy match found:
        ```{python}
        helpers.gt_style(df_inp=fuzzy_matched)
        ```

        no matches found:
        ```{python}
        helpers.gt_style(df_inp=fuzzy_matched_none)
        ```

        """

        # ----- Init variables ----- #
        fuzzy_matched = pl.DataFrame()
        fuzzy_unmatched = pl.DataFrame()

        # ------- Fuzzy Matching ------- #

        if dob_match.height > 0:
            multiple_matches_ratios = self.score(dob_match.lazy()).collect()

            # Get ones that matched on ratio >= threshold
            multiple_matches_ratios_final = multiple_matches_ratios.filter(
                pl.col('match_ratio').ge(self.threshold) | pl.col('reverse_match_ratio').ge(self.threshold)
            )

            # get the top matches of the groups with no score meeting the threshold
            fuzzy_unmatched = (
                multiple_matches_ratios
                # Remove any groups that had a match >= the threshold
                .join(multiple_matches_ratios_final, on=self.key, how='anti')
                # Get the max between the two ratio methods
                .with_columns(pl.max_horizontal('match_ratio', 'reverse_match_ratio').alias('max_ratio'))
                # Select the match with the highest ratio within each group
                .group_by(self.key)
                .agg(pl.all().sort_by('max_ratio', descending=True).first())
                .drop('max_ratio')
            )

            # here we need to group by key and select row with the closest collection date difference
            fuzzy_matched = (
                multiple_matches_ratios_final
                .with_columns(
                    # Get a date range calculation of days between submitted collection date and ref collection date
                    business_day_count=pl.business_day_count("submitted_collection_date", "reference_collection_date").abs()
                )
                .group_by(pl.col(self.key))
                .agg(pl.all().sort_by('business_day_count').first())
            )
        
        if fuzzy_matched.height==0:
            fuzzy_matched = pl.DataFrame()

        return fuzzy_matched, fuzzy_unmatched

    def fuzzZ(self, verbose=True):
        
        # Process the Submissions to Fuzzy
        ref_prep, submissions_to_fuzzy_prep = self.clean_all()
        # Split by presence of demographics and specimen collection date
        fuzzy_with_demo, fuzzy_without_demo = self.filter_demo(submissions_to_fuzzy_prep)
        # find exact matches
        exact_matched, dob_match = self.find_exact_match(ref_prep, fuzzy_with_demo)
        # find fuzzy matches
        fuzzy_matched, fuzzy_unmatched = self.fuzzy_match(dob_match)
        # # print summary
        # if verbose:
        #     self.__output_summary(submissions_to_fuzzy_df, fuzzy_matched_review, fuzzy_without_demo, fuzzy_matched_none,
        #                fuzzy_matched_roster)
        # return fuzzy outputs
        return exact_matched, fuzzy_matched, fuzzy_unmatched, fuzzy_without_demo
