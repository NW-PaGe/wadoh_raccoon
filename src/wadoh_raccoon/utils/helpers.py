import polars as pl
import paramiko
from io import BytesIO
from datetime import datetime
from datetime import date
from great_tables import GT, md, style, loc, google_font
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def clean_name(col: str) -> pl.Expr:
    """
    Clean name field by stripping non-alpha characters and converting to uppercase.

    Parameters
    ----------
    col: str
        Name of column to clean

    Returns
    -------
    pl.Expr:
        a column of uppercase, non-alpha character, non-whitespace strings

    Examples
    --------
    ```{python}
    import polars as pl
    from wadoh_raccoon.utils import helpers

    df = pl.DataFrame({
        "name": [
            "A$AP rocky",
            "50 cent",
            "sTevIe WoNdEr"
        ]
    })

    output = df.with_columns(helpers.clean_name("name").alias("clean_name"))

    helpers.gt_style(df_inp=output)

    ```
    """
    return pl.col(col).str.replace_all('[^a-zA-Z]', '').str.to_uppercase()

def date_format(df: pl.DataFrame | pl.LazyFrame,col: str):
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
    df: pl.DataFrame
        a polars dataframe (needed to check if col is pl.Date type or not)
    col: str
        a string column that has a date


    Returns
    -------
    pl.Expr:
        a date column

    Examples
    --------
    ```{python}
    import polars as pl
    from wadoh_raccoon.utils import helpers

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

    output = (
        df
        .with_columns(
            new_date=helpers.date_format(df=df,col='dates')
        )
    )

    helpers.gt_style(df_inp=output)

    ```

    """
    # return (
    #     pl.when(pl.col(col).is_(pl.Date))
    #     .then(pl.lit('date')).alias("check")
    # )

    if isinstance(df, pl.DataFrame):
        col_type = df.schema[col]
    else:
        col_type = df.collect_schema()[col]
    if col_type.is_temporal():
        return pl.col(col).cast(pl.Date)


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
        )

def get_secrets(vault, keys):
    """ Get secrets

    Retrieve secrets from Azure KeyVault.
    This function will utilize the keys that are passed to retrieve the 
    corresponding secrets.

    **Note: Authenication takes place via DefaultAzureCredential which attempts
    multiple authentication methods. One method is checking against Azure CLI 
    if logged in.
    
    Usage
    -----
    Use this function to securely retrieve secret values from Azure KeyVault
    using the specified key(s). The function accepts either a single key or
    multiple keys as a list.
    
    Parameters
    ----------
    vault: str
        Key vault url.
    keys: str or list of str
        A single secret key or list of secret keys.

    
    Returns
    -------
    str or tuple of str
        If a single key is provided, returns the secret value as a string.
        If a list of keys is provided, returns a tuple of secret values in the 
        same order.
    
    Examples
    --------
    ```python
    from wadoh_raccoon.utils import helpers

    # Get a single secret
    db_password = helpers.get_secrets("keyvault_url", "db-password")
    
    # Get multiple secrets at once
    username, password, api_key = helpers.get_secrets(
        "keyvault_url",
        ["db-username", "db-password", "api-key"]
    )
    ```
    """
    # Init credential and client
    credential = DefaultAzureCredential()
    vault_url = vault
    client = SecretClient(vault_url=vault_url, credential=credential)
    
    # Handle single string input
    if isinstance(keys, str):
        return client.get_secret(keys).value
    
    # Handle list input
    return tuple(client.get_secret(key).value for key in keys)


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
    import polars as pl
    from wadoh_raccoon.utils import helpers

    data = pl.DataFrame({
        "lab_name": ["PHL", "MFT", "ELR","PHL"],
        "first_name": ["Alice", "Bob", "Charlie", "Charlie"],
        "last_name": ["Smith", "Johnson", "Williams", "Williams"],
        "WA_ID": [1,2,4,4]
    })
    
    received_submissions_df = (
            helpers.save_raw_values(df_inp=data,primary_key_col="WA_ID")
    )

    helpers.gt_style(data)
    
    ```

    ```{python}
    helpers.gt_style(received_submissions_df)
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


def gt_style(
    df_inp: pl.DataFrame,
    title: str="",
    subtitle: str="",
    add_striping_inp=True,
    index_inp=True
):

    """ Style for GT Tables

    Usage
    -----
    Apply this style to a Polars DataFrame

    Parameters
    ----------
    df_inp: pl.DataFrame
        a polars dataframe
    title: str
        a title for the table (optional)
    subtitle: str
        a subtitle for the table (optional, must have a title if using a subtitle)
    add_striping_inp: bool
        striping in the table True or False
    index_inp: bool
        add a column for the row number and label it `index`

    Returns
    -------
    : GT
        a GT object (great_tables table)
    

    Examples
    --------
    ```{python}
    import polars as pl
    from wadoh_raccoon.utils import helpers
    df = pl.DataFrame({
        "x": [1,1,2],
        "y": [1,2,3]
    })

    ```
    A table with a title/subtitle:
    
    ```{python}
    helpers.gt_style(df_inp=df,title="My Title",subtitle="My Subtitle")

    ```

    No title/subtitle
    ```{python}
    helpers.gt_style(df_inp=df)
    ```

    Without an index:
    ```{python}
    helpers.gt_style(df_inp=df,index_inp=False)
    ```

    Without striping:
    ```{python}
    helpers.gt_style(df_inp=df,add_striping_inp=False)
    ```

    """
    # Check for title and subtitle, and conditionally add them
    table = (
        GT(
            df_inp.with_row_index() if index_inp else df_inp,  # Add row index only if index_inp is True
            rowname_col="index" if index_inp else None
        )
        # .opt_vertical_padding(scale=1)
        # .opt_stylize(add_row_striping=add_striping_inp,color='black')
        # .opt_row_striping()
        .opt_table_font(font=google_font(name="JetBrains Mono"))
        .opt_table_outline(color='#0c0909')
        .tab_style(
            style=[
                style.borders(sides=['bottom'],weight='2px',color='black')
            ],
            locations=loc.column_header()
        )
        .tab_style(
            style=[
                style.borders(sides=['bottom'],weight='2px',color='black')
            ],
            locations=loc.stubhead()
        )
        .tab_style(
            style=[
                style.fill(color="#f9e3d6")
            ],
            locations=[loc.stub(),loc.stubhead()]
        )
    )
    
    if title and subtitle:
        table = table.tab_header(title=md(title), subtitle=md(subtitle))
    
    elif title:
        table = table.tab_header(title=md(title))

    if add_striping_inp:
        table = table.opt_row_striping()

    if index_inp:
        table = table.tab_stubhead(label="index")


    return table

def mft_upload(
    upload: pl.DataFrame,
    dir: str,
    upload_file_name: str,
    upload_file_extension: str,
    username: str,
    password: str,
    host: str = "mft.wa.gov"
) -> None:
    """Upload files to Washington State MFT server

    Upload Polars DataFrames to the Washington State Managed File Transfer (MFT) 
    server via SFTP. This function converts DataFrames to various file formats 
    and securely transfers them to specified directories on the MFT server.

    **Note: Authentication requires explicit credentials to be provided. The 
    function automatically adds the server's host key for simplified connection 
    handling.
    
    Usage
    -----
    Use this function to upload processed surveillance data, reports, or other 
    DataFrames to the MFT server for sharing with partners. 
    The function handles file format conversion.
    
    Parameters
    ----------
    upload : polars.DataFrame
        The Polars DataFrame to upload.
    dir : str
        Target directory path on the MFT server (e.g., '/outbound/partner').
    upload_file_name : str
        Name of the file without extension (e.g., 'surveillance_report').
    upload_file_extension : str
        File extension including the dot. Supported formats: '.csv', '.xlsx', 
        '.json', '.parquet'.
    username : str
        MFT server username.
    password : str
        MFT server password.
    host : str, optional
        MFT server hostname. Default is 'mft.wa.gov'.
    
    Returns
    -------
    None
        Files are uploaded directly to the MFT server. Success message printed.
    
    Raises
    ------
    TypeError
        If upload is not a Polars DataFrame.
    ValueError
        If upload is empty, required parameters are missing, or 
        upload_file_extension is not supported.
    OSError
        If the target directory does not exist or cannot be accessed on the 
        MFT server.
    ConnectionError
        If SFTP connection fails.
    
    Examples
    --------
    ```python
    import polars as pl
    from wadoh_raccoon.utils.helpers import mft_upload, get_secrets
    
    # Create sample DataFrame
    df = pl.DataFrame({
        'case_id': [1, 2, 3],
        'pathogen': ['Salmonella', 'E. coli', 'Campylobacter']
    })
    
    # Get credentials from Key Vault
    mft_user, mft_pass = get_secrets(
        'vault_url', 
        ['mft-username', 'mft-password']
    )
    
    # Upload as CSV
    mft_upload(
        upload=df,
        dir='DEV_TESTING',
        upload_file_name='weekly_report_2024_01',
        upload_file_extension='.csv',
        username=mft_user,
        password=mft_pass
    )
    
    # Upload as Excel
    mft_upload(
        upload=df,
        dir='DEV_TESTING',
        upload_file_name='weekly_report_2024_01',
        upload_file_extension='.xlsx',
        username=mft_user,
        password=mft_pass
    )
    ```
    """
    
    # Input validation
    if not isinstance(upload, pl.DataFrame):
        raise TypeError(f"upload must be a Polars DataFrame, got {type(upload)}")
    
    if upload.is_empty():
        raise ValueError("Cannot upload empty DataFrame")
    
    if not dir or not upload_file_name:
        raise ValueError("dir and upload_file_name cannot be empty")
    
    # Supported file extensions
    supported_extensions = {'.csv', '.xlsx', '.json', '.parquet'}
    if upload_file_extension not in supported_extensions:
        raise ValueError(
            f"Unsupported file type: {upload_file_extension}. "
            f"Supported formats: {', '.join(sorted(supported_extensions))}"
        )
    
    # Convert DataFrame to bytes based on extension
    try:
        if upload_file_extension == ".csv":
            upload_file = upload.write_csv().encode('utf-8')
        elif upload_file_extension == ".xlsx":
            buffer = BytesIO()
            upload.write_excel(buffer)
            upload_file = buffer.getvalue()
        elif upload_file_extension == ".json":
            upload_file = upload.write_json().encode('utf-8')
        elif upload_file_extension == ".parquet":
            buffer = BytesIO()
            upload.write_parquet(buffer)
            upload_file = buffer.getvalue()
        else:
            # Should never reach here due to earlier validation
            raise ValueError(f"Unsupported extension: {upload_file_extension}")
    except Exception as e:
        raise ValueError(f"Failed to convert DataFrame to {upload_file_extension}: {e}")
    
    # Establish SFTP connection and upload
    client = paramiko.SSHClient()
    
    try:
        # Automatically add host keys
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect to MFT server
        client.connect(
            hostname=host,
            username=username,
            password=password
        )
        
        # Open SFTP session
        sftp = client.open_sftp()
        
        try:
            # Define upload path
            upload_path = f"{dir}/{upload_file_name}{upload_file_extension}"
            
            # Write file to remote server (binary mode)
            with sftp.open(upload_path, 'wb') as remote_file:
                remote_file.write(upload_file)
            
            print(f"Successfully uploaded {upload.shape[0]} rows to {upload_path}")
            
        except (OSError, IOError) as e:
            raise OSError(
                f"Failed to upload to {upload_path}. "
                f"Verify directory '{dir}' exists and is accessible. Error: {e}"
            )
        finally:
            sftp.close()
            
    except paramiko.AuthenticationException:
        raise ConnectionError("Authentication failed. Check username and password.")
    except paramiko.SSHException as e:
        raise ConnectionError(f"SSH connection error: {e}")
    except Exception as e:
        raise ConnectionError(f"Unexpected error during SFTP upload: {e}")
    finally:
        client.close()
        
def lazy_height(lf: pl.DataFrame | pl.LazyFrame):
    """Output the height of a polars frame regardless of it being lazy or eager"""
    if isinstance(lf, pl.LazyFrame):
        return lf.select(pl.len()).collect().item()
    else:
        return lf.height

