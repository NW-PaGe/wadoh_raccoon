from azure.identity import AzureCliCredential
from azure.core.credentials import TokenCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient


def blob_upload(account: str,
                container_name: str,
                blob_path: str,
                file_path: str,
                credential: TokenCredential = None,
                overwrite: bool = True,
                access_tier: str = None,
                account_is_url: bool = False):
    """
    Uploads a local file to Azure Blob Storage.

    This method uploads a file from a specified local directory (`file_path`) to a specified
    blob directory (`blob_path`) in Azure Blob Storage.

    Args:
        account (str): the storage account name.
        container_name (str): the storage container name.
        blob_path (str): The path and name of the blob to be created in Azure Blob Storage.
        file_path (str): The path and name of the file to upload.
        credential (TokenCredential, optional): The Azure credential used for authentication.
                                                Can be any implementation of `azure.core.credentials.TokenCredential`
                                                (e.g., `DefaultAzureCredential`, `AzureCliCredential`,
                                                `ManagedIdentityCredential`). Defaults to `AzureCliCredential`.
        overwrite (bool, optional): Whether blobs should be overwritten if they exist. Defaults to True.
        access_tier (str, optional): The access tier for the blob ('Hot', 'Cool', 'Cold' or 'Archive').
                                     If None, the default access tier will be used.
        account_is_url (bool, optional): Whether `account` is supplied as a full URL instead of an account name.
                                         Account URLs will be set as the account endpoint as-is, and should be used
                                         for testing with Azurite, for example. Account names can be supplied with
                                         this flag set to False, and will be constructed to a full URL in the form
                                         of `https://{account}.blob.core.windows.net`. Defaults to False.

    Returns:
        None: This method uploads the specified file to Azure Blob Storage and prints the
              result, but does not return any value.

    Example:
        # Upload a file from a local directory to the blob storage:
        # from wadoh_raccoon.utils.azure import blob_upload
        # blob_upload(
        #     account="mystorageaccount",
        #     container_name="mycontainer",
        #     file_path="data.csv",
        #     blob_path="myblob/blob_data.csv"
        # )
    """
    # Validate access tier value
    if access_tier and access_tier.capitalize() not in ['Hot', 'Cool', 'Cold', 'Archive']:
        print(f'access_tier: {access_tier}')
        raise ValueError(f"access_tier must be one of: 'Hot', 'Cool', 'Cold', 'Archive', None")

    # Use Azure CLI creds to authenticate if not otherwise provided
    # NOTE: you will need to log in via the Azure CLI before this will work
    # Open your terminal and run `az login`; you may need to specify a specific tenant with `az login --tenant tenantid`
    if credential is None:
        credential = AzureCliCredential()
    # Set account url
    if not account_is_url:
        account = f'https://{account}.blob.core.windows.net'
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=account, credential=credential, connection_timeout=300)
    # Get container client for file upload
    container_client = blob_service_client.get_container_client(container_name)
    # Connect to blob client
    blob_client = container_client.get_blob_client(blob_path)

    # Skip blob writing if a blob already exists (and should not be overwritten)
    if blob_client.exists() and not overwrite:
        print(f"Blob '{blob_path}' already exists. Skipping upload.")
        return

    # Write data to blob
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"File '{file_path}' uploaded to Blob storage as '{blob_path}'")

    # Set the access tier, if specified
    if access_tier:
        blob_client.set_standard_blob_tier(access_tier)
        print(f'Blob access tier set: {access_tier.capitalize()}')


# Private method to be called from blob_delete
def __delete(client: ContainerClient,
             blob_path: str,
             recursive: bool = False):
    """
    Deletes blobs from the specified directory in Azure Blob Storage.

    This private helper method is designed to iterate through blobs within a given directory
    (`blob_path`) in Azure Blob Storage. It deletes blobs one by one, and if a subdirectory is
    found and the `recursive` flag is set to True, it continues the deletion process for that
    subdirectory.

    Args:
        client (ContainerClient): The Azure Blob Storage container client used to interact
                                   with the blob container.
        blob_path (str): The path to the directory (blob) to delete from. This can be a
                        directory or a specific blob.
        recursive (bool, optional): If True, subdirectories within the `blob_path` will also
                                     be processed and deleted. Defaults to False.

    Returns:
        None: This method performs deletions and prints the results but does not return any value.

    Example:
        # Delete a specific blob:
        # __delete(client=container_client, blob_path="blob/to_delete/data.csv")
        # Delete a blob from a directory and any blobs in subdirectories:
        # __delete(client=container_client, blob_path="blob/to_delete", recursive=True)
        
    """
    blobs = client.walk_blobs(name_starts_with=blob_path)
    for blob in blobs:
        # Check if blob is a subdirectory or not
        if blob.name[-1:] != '/':
            blob_client = client.get_blob_client(blob.name)
            blob_client.delete_blob()
            print(f"File '{blob.name}' deleted from Blob storage.")
        elif recursive:
            __delete(client, blob.name, recursive=True)


# Method used to delete files from blob storage.
def blob_delete(account: str,
                container_name: str,
                blob_path: str,
                credential: TokenCredential = None,
                recursive: bool = False,
                account_is_url: bool = False):
    """
    Deletes a specific file or all files within a directory in Azure Blob Storage.

    This method establishes a connection to the specified Azure Blob Storage container and 
    deletes either a specific file or all files within a given directory. If the `recursive` 
    flag is set to True, it will delete files from subdirectories as well.

    Args:
        account (str): the storage account name.
        container_name (str): the storage container name.
        blob_path (str): The directory (or blob) path from which files are to be deleted.
                         This should be a directory path if deleting multiple files.
        credential (TokenCredential, optional): The Azure credential used for authentication.
                                                Can be any implementation of `azure.core.credentials.TokenCredential`
                                                (e.g., `DefaultAzureCredential`, `AzureCliCredential`,
                                                `ManagedIdentityCredential`). Defaults to `AzureCliCredential`.
        recursive (bool, optional): If True, all files within subdirectories of `blob_path` will also be deleted. 
                                    Defaults to False.
        account_is_url (bool, optional): Whether `account` is supplied as a full URL instead of an account name.
                                         Account URLs will be set as the account endpoint as-is, and should be used
                                         for testing with Azurite, for example. Account names can be supplied with
                                         this flag set to False, and will be constructed to a full URL in the form
                                         of `https://{account}.blob.core.windows.net`. Defaults to False.

    Returns:
        None: This method deletes the specified file(s) or directory contents, and prints the
              results but does not return any value.

    Example:
        # Delete a specific blob file:
        # blob_delete(account="myaccount",
        #             container_name="mycontainer",
        #             blob_path="blob/to_delete/data.txt")

        # Delete files within a directory and any files in subdirectories:
        # blob_delete(account="myaccount",
        #             container_name="mycontainer",
        #             blob_path="blob/to_delete/", 
        #             recursive=True)
    """


    # Use Azure CLI creds to authenticate if not otherwise provided
    # NOTE: you will need to log in via the Azure CLI before this will work
    # Open your terminal and run `az login`; you may need to specify a specific tenant with `az login --tenant tenantid`
    if credential is None:
        credential = AzureCliCredential()

    # Set account url
    if not account_is_url:
        account = f'https://{account}.blob.core.windows.net'
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=account, credential=credential)
    # Get container client for file upload
    container_client = blob_service_client.get_container_client(container_name)

    # Remove specific blob
    if not recursive:
        blob_client = container_client.get_blob_client(blob_path)
        blob_client.delete_blob()
        print(f"File '{blob_path}' deleted from Blob storage.")
    # Otherwise, remove all the files in the blob_path
    else:
        __delete(container_client, blob_path, recursive=recursive)


# Method for downloading a file from Blob storage to a locally-accessible drive
def blob_download(account: str,
                  container_name: str,
                  blob_path: str,
                  file_path: str,
                  credential: TokenCredential = None,
                  account_is_url: bool = False):
    """
    Downloads a specific file from Azure Blob Storage to a local directory.

    This method authenticates with Azure using the Azure CLI, establishes a connection to the
    specified Azure Blob Storage container, and downloads a specific file from a given directory in
    Azure Blob Storage.

    Args:
        account (str, optional): The storage account name.
        container_name (str, optional): The storage container name.
        blob_path (str): The blob path and name in Azure Blob Storage.
        file_path (str): The local path and name where the file will be saved.
                         The path must exist or will be created automatically.
        credential (TokenCredential, optional): The Azure credential used for authentication.
                                                Can be any implementation of `azure.core.credentials.TokenCredential`
                                                (e.g., `DefaultAzureCredential`, `AzureCliCredential`,
                                                `ManagedIdentityCredential`). Defaults to `AzureCliCredential`.
        account_is_url (bool, optional): Whether `account` is supplied as a full URL instead of an account name.
                                         Account URLs will be set as the account endpoint as-is, and should be used
                                         for testing with Azurite, for example. Account names can be supplied with
                                         this flag set to False, and will be constructed to a full URL in the form
                                         of `https://{account}.blob.core.windows.net`. Defaults to False.

    Returns:
        None: This method downloads the specified file from Azure Blob Storage to the local machine
              and prints the result, but does not return any value.

    Example:
        # Download a specific file from Azure Blob Storage:
        # blob_delete(account="myaccount",
        #             container_name="mycontainer",
        #             blob_path="blob/to_download/data.json",
        #             file_path="data/data.json")

    """
    # Use Azure CLI creds to authenticate if not otherwise provided
    # NOTE: you will need to log in via the Azure CLI before this will work
    # Open your terminal and run `az login`; you may need to specify a specific tenant with `az login --tenant tenantid`
    if credential is None:
        credential = AzureCliCredential()

    # Set account url
    if not account_is_url:
        account = f'https://{account}.blob.core.windows.net'
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=account, credential=credential, connection_timeout=300)
    # Get container client for file upload
    container_client = blob_service_client.get_container_client(container_name)
    # Connect to blob client
    blob_client = container_client.get_blob_client(blob_path)

    # Download blob data and write to local path
    with open(file_path, "wb") as data:
        download_stream = blob_client.download_blob()
        data.write(download_stream.readall())
        print(f"File '{blob_path}' downloaded as '{file_path}'")
