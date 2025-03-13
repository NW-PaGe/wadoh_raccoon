import polars as pl
from datetime import datetime
from datetime import date

def date_format(col: str,output_date_name = "output_date_name"):
    """ Format Dates

    Convert string dates into a yyyy-mm-dd format. 
    The function uses pl.coalesce to try to process different formats.
    For example, it will first try to convert m/d/y, and then if that doesn't work it will try d/m/y.
    It's not perfect, but if someone messes up the date it's their fault.
    
    **Note: it won't attempt to convert excel dates. If someone sends us excel dates we will file a lawsuit.**

    Usage
    -----
    To be applied to a string date column.

    Parameters
    ----------
    col: str
        a string column that has a date

    Returns
    -------
    output_date: date
        a date column
    
    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    ```{python}
    import polars as pl
    from src.subtype_link.utils.helpers import date_format


    df = pl.DataFrame({
        "dates": [
            "2024-10-30",     # ISO format
            "30/10/2024",     # European format
            "10/20/2024",     # US format
            "10-30-2024",     # US format
            "October 30, 2024",  # Full month name format,
            "45496",           # an excel date LOL
            "2022-12-27 08:26:49"
        ]
    })
    
    print(
        df
        .with_columns(
            new_date=date_format('dates','new_date')
        )
    )
    
    ```

    """
    return pl.coalesce(
            # see this for date types https://docs.rs/chrono/latest/chrono/format/strftime/index.html
            # regular dates like sane people yyyy-mm-dd
            pl.col(col).str.strptime(pl.Date, "%F", strict=False),
            # datetimes - semi sane
            pl.col(col).str.strptime(pl.Date, "%F %T", strict=False),
            # m/d/y - gettin wild
            pl.col(col).str.strptime(pl.Date, "%D", strict=False),
            # dont even ask
            pl.col(col).str.strptime(pl.Date, "%c", strict=False),
            # mm-dd-yyyy
            pl.col(col).str.strptime(pl.Date, "%m-%d-%Y", strict=False),
            # dd-mm-yyyy
            pl.col(col).str.strptime(pl.Date, "%d-%m-%Y", strict=False),
            # mm/dd/yyyy
            pl.col(col).str.strptime(pl.Date, "%m/%d/%Y", strict=False),
            # dd/mm/yyyy
            pl.col(col).str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            # if someone literally writes out the month. smh
            pl.col(col).str.strptime(pl.Date, "%B %d, %Y", strict=False),
            # if someone sends an excel date we'll just reject it and call the cops on them

        ).alias(output_date_name)
        

def save_raw_values(df_inp: pl.DataFrame, primary_key_col: str):
    """ save raw values

    Usage
    -----
    Converts a polars dataframe into a dataframe with all columns in a struct column.
    It's good for saving raw outputs of data.

    Parameters
    ----------
    df_inp: pl.DataFrame
        a polars dataframe
    primary_key_col: str
        column name for the primary key (submission key, not person/case key)

    Returns
    -------
    df: pl.DataFrame
        a dataframe
    
    Examples
    --------
    ```{python}
    #| echo: false
    {{< include "../_setup.qmd" >}}
    ```
    ```{python}
    import polars as pl
    from test_project import helpers

    data = pl.DataFrame({
        "lab_name": ["PHL", "MFT", "ELR","PHL"],
        "first_name": ["Alice", "Bob", "Charlie", "Charlie"],
        "last_name": ["Smith", "Johnson", "Williams", "Williams"],
        "WA_ID": [1,2,4,4]
    })
    
    received_submissions_df = (
            helpers.save_raw_values(df_inp=data,primary_key_col="WA_ID")
    )

    helpers.gt_style(df_inp=data)
    
    ```

    ```{python}
    helpers.gt_style(df_inp=received_submissions_df)
    ```

    """

    df = (
        df_inp
        .select([
            # save the primary key
            pl.col(primary_key_col).alias('submission_number'),

            # internal create date
            pl.lit(date.today()).alias("internal_create_date"),

            # save a copy of all the original columns and put them into a struct column
            pl.struct(pl.all()).alias("raw_inbound_submission")
        ])
    )

    return df