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
            # now drop the original name columns to clean up the namespace
            # .select(pl.exclude([first_name, last_name, spec_col_date, dob]))
        )

        return clean_df

    @staticmethod
    def __demo_param_checker(param, param_src, param_ref, param_name):
        if param is None and (param_src is None or param_ref is None):
            raise ValueError(f"`{param_name}` or both `{param_name}_src` and `{param_name}_ref` must not be None")

    def clean_all(self) -> pl.DataFrame:

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
                date_subtract = (pl.col('submitted_collection_date') - pl.col('reference_collection_date'))
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


    def fuzzy_match(self, exact_match, dob_match):
        """ 

        Where the magic happens. Do the fuzzy matching to the dataframe

        Parameters
        ----------
        exact_match: pl.DataFrame
            the dataframe that has exact matches to the reference
        dob_match: pl.DataFrame
            the dataframe that has records grouped by their dob match
        
        Returns
        ----------
        fuzzy_with_demo: pl.DataFrame
            dataframe with all of first name, last name, and dob filled out
        fuzzy_without_demo: pl.DataFrame
            dataframe that has records with one of first name, last name, or dob missing
        
        Examples
        --------
        ```{python}
        # Not to be run like this
        # Only for demonstration!!

        submissions_to_fuzzy_df = instance._fuzzy_process__submissions_to_fuzzy()
        ref, submissions_to_fuzzy_prep = instance._fuzzy_process__transform(submissions_to_fuzzy_df)
        fuzzy_with_demo, fuzzy_without_demo = instance._fuzzy_process__filter_demo(submissions_to_fuzzy_prep)
        exact_match, dob_match = instance._fuzzy_process__find_exact_match(ref,fuzzy_with_demo)

        fuzzy_matched_review, fuzzy_matched_none, fuzzy_matched_roster = instance._fuzzy_process__fuzzy_match(exact_match, dob_match)
        ```
        
        Fuzzy match found:
        ```{python}
        helpers.gt_style(df_inp=fuzzy_matched_review)
        ```


        no matches found:
        ```{python}
        helpers.gt_style(df_inp=fuzzy_matched_none)
        ```

        exact demographic matches:
        ```{python}
        helpers.gt_style(df_inp=fuzzy_matched_roster)
        ```

        
        """

        # ----- Init variables ----- # 
        fuzzy_review = pl.DataFrame()
        fuzzy_unmatched = pl.DataFrame()
        fuzzy_matched = pl.DataFrame()

        # ------- Fuzzy Matching ------- #

        if dob_match.select(pl.len())[0,0] > 0:
            multiple_matches_ratios = (
                dob_match
                .lazy()
                .with_columns(
                
                    # Get a name string for grouping
                    pl.concat_str(pl.col('first_name_clean'),pl.col('last_name_clean')).alias('combined_name'),

                    # First get the fuzz ratio with the first name
                    pl.struct(['first_name_clean','first_name_clean_right'])
                    .map_elements(
                        lambda cols: fuzz.ratio(cols['first_name_clean'],cols['first_name_clean_right']),
                        skip_nulls=False,
                        return_dtype=pl.Int64
                    )
                    .alias('first_name_result'),

                    # Now get the fuzz ratio with the last name
                    pl.struct(['last_name_clean','last_name_clean_right'])
                    .map_elements(
                        lambda cols: fuzz.ratio(cols['last_name_clean'],cols['last_name_clean_right']),
                        skip_nulls=False,
                        return_dtype=pl.Int64
                        )
                    .alias('last_name_result'),

                    # Now reverse - WDRS is known to switch first and last names
                    # First get the fuzz ratio with the first name
                    pl.struct(['first_name_clean','last_name_clean_right'])
                    .map_elements(
                        lambda cols: fuzz.ratio(cols['first_name_clean'],cols['last_name_clean_right']),
                        skip_nulls=False,
                        return_dtype=pl.Int64
                        )
                    .alias('reverse_first_name_result'),

                    # Now get the fuzz ratio with the last name
                    pl.struct(['last_name_clean','first_name_clean_right'])
                    .map_elements(
                        lambda cols: fuzz.ratio(cols['last_name_clean'],cols['first_name_clean_right']),
                        skip_nulls=False,
                        return_dtype=pl.Int64
                        )
                    .alias('reverse_last_name_result'),

                )
                .with_columns(
                    # Now get the ratios between first and last name matches
                    # pl.struct(['first_name_result','last_name_result'])
                    ((pl.col('first_name_result') + pl.col('last_name_result')) / 2).alias('match_ratio'),
                    ((pl.col('reverse_first_name_result') + pl.col('reverse_last_name_result')) / 2).alias('reverse_match_ratio')
                )
                # Mark columns that have ratio > 90
                .with_columns(
                    pl.when(pl.col('match_ratio') >= self.threshold).then(1)
                    .otherwise(0).alias('matched'),

                    pl.when(pl.col('reverse_match_ratio') >= self.threshold).then(1)
                    .otherwise(0).alias('reverse_matched')
                )
                
            ).collect()

            # Get ones that matched on ratio > 90
            multiple_matches_ratios_final = (
                multiple_matches_ratios.filter((pl.col('matched') == 1) | (pl.col('reverse_matched') == 1))
            )

            # get all the matches above 60
            fuzzy_unmatched = (
                multiple_matches_ratios
                .filter((pl.col('matched') != 1) & (pl.col('reverse_matched') != 1))
                # remove any that were already matched - the filter above will still include bad matches
                .join(multiple_matches_ratios_final, on=self.key, how='anti')
                .with_columns(
                    pl.when((pl.col('match_ratio')>60) | (pl.col('reverse_match_ratio')>60)).then(1)
                    .otherwise(0).alias('match_ratio_above_60'),

                )
                .group_by(self.key)
                .agg([pl.all().sort_by(['match_ratio_above_60'],descending=True).max()])
            )

            temp_mult_matches = (
                multiple_matches_ratios_final
                .with_columns(
                
                    # Get a name string for grouping
                    pl.concat_str(pl.col('first_name_clean'),pl.col('last_name_clean')).alias('combined_name'),

                    # Get a date range calculation of days between submitted collection date and collection date in WDRS
                    # date_difference.alias('date_difference')
                    business_day_count=pl.business_day_count("submitted_collection_date", "reference_collection_date"),

                )
            )

            # print(temp_mult_matches.columns)

            # here we need to group by sub_number and select the closest collection date difference
            # then join back to the temp_mult_matches df to get all the og columns
            fuzzy_review = (
                temp_mult_matches
                .group_by(pl.col(self.key))
                .agg(pl.col('business_day_count').min())

                # join back to gett all original cols join_nulls=True is nulls_equal=True in newer Polars versions
                # NOTE: you need the join_nulls or nulls_equal=True because if a record has missing ref_collection_date
                # then this join WILL NOT work because business_day_count will be null. it will return a dataframe that has all nulls for all cols
                .join(temp_mult_matches,on=[self.key, 'business_day_count'],how='left',join_nulls=True)
                # often times subtypes have a ton of rows in WDRS, just take the unique ones
                .unique(maintain_order=True)
            )
        
        if fuzzy_review.select(pl.len())[0,0]==0:
            fuzzy_review = pl.DataFrame()

        # ------- Format Exact Matches ------- #

        if exact_match.select(pl.len())[0,0] > 0:
            fuzzy_matched = (
                exact_match
            )
        else: 
            fuzzy_matched = pl.DataFrame()

        # breakpoint()

        return fuzzy_review, fuzzy_unmatched, fuzzy_matched
    
    # TODO: Full matching goes here?

    def fuzzZ(self, verbose=True):
        
        # Process the Submissions to Fuzzy
        ref_prep, submissions_to_fuzzy_prep = self.clean_all()
        # Split by presence of demographics and specimen collection date
        fuzzy_with_demo, fuzzy_without_demo = self.filter_demo(submissions_to_fuzzy_prep)
        # find exact matches
        exact_match, dob_match = self.find_exact_match(ref_prep, fuzzy_with_demo)
        # find fuzzy matches
        fuzzy_review, fuzzy_unmatched, fuzzy_matched = self.fuzzy_match(exact_match, dob_match)
        # # print summary
        # if verbose:
        #     self.__output_summary(submissions_to_fuzzy_df, fuzzy_matched_review, fuzzy_without_demo, fuzzy_matched_none,
        #                fuzzy_matched_roster)
        # return fuzzy outputs
        return fuzzy_matched, fuzzy_unmatched, exact_match, fuzzy_without_demo, fuzzy_review