import pytest
import socket
from pathlib import Path
import tempfile
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from wadoh_raccoon.utils import azure

# These tests rely on an active azurite session listening at http://127.0.0.1:10000
# See https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#install-azurite
# for details on installing azurite via npm, docker hub, github, or vs/vs code. See
# https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#run-azurite for
# details on running azurite.

# Constants for testing
AZURITE_HOST = "127.0.0.1"
AZURITE_PORT = 10000
AZURITE_URL = f"http://{AZURITE_HOST}:{AZURITE_PORT}/devstoreaccount1"
CONTAINER = "testcontainer"
BLOB_DIR = "blob"
BLOB = f"{BLOB_DIR}/test.txt"
BLOB_2 = f"{BLOB_DIR}/another_test.txt"
BLOB_3 = "another_blob/test.txt"
BLOB_DATA = b"Testing Blob Upload"
CONNECTION_STRING = (
    # Use Azurite default dev credentials
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


@pytest.fixture(scope="module", autouse=True)
def check_azurite():
    '''Test whether azurite is listening as the specified host and port'''
    try:
        with socket.create_connection((AZURITE_HOST, AZURITE_PORT), timeout=5):
            pass  # Connection succeeded
    except Exception as e:
        pytest.fail(f"Could not connect to Azurite at {AZURITE_HOST}:{AZURITE_PORT}. Error: \n{e}")

@pytest.fixture
def local_file():
    '''Create a temporary file containing BLOB_DATA'''
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(BLOB_DATA)
    temp_file.close()
    yield temp_file.name
    Path(temp_file.name).unlink(missing_ok=True)

@pytest.fixture(scope="module")
def service_client():
    '''Create a service client object to be shared by all the azure tests'''
    return BlobServiceClient.from_connection_string(conn_str=CONNECTION_STRING)

@pytest.fixture(scope="module")
def credential(service_client):
    '''Create a credentials object to be shared by all the azure tests'''
    return service_client.credential

@pytest.fixture
def container_client(service_client):
    '''Create a container client for each test'''
    # Initialize container (if not done so already)
    service_client.create_container(CONTAINER)
    # Initialize the container client
    yield service_client.get_container_client(CONTAINER)
    # Remove the container
    try:
        service_client.delete_container(CONTAINER)
    except ResourceNotFoundError:
        pass


def test_blob_upload(local_file, credential, container_client):
    '''Test uploading a file to blob storage'''
    # Upload a file to azurite emulated blob storage
    azure.blob_upload(
        account=AZURITE_URL,
        container_name=CONTAINER,
        blob_path=BLOB,
        file_path=local_file,
        credential=credential,
        overwrite=True,
        account_is_url=True
    )
    # Read the uploaded data
    uploaded_data = container_client.get_blob_client(BLOB).download_blob().readall()
    # Test that the uploaded data matches BLOB_DATA
    assert uploaded_data == BLOB_DATA, ("blob_upload failed."
                                        f"\nExpected: `{BLOB_DATA}`"
                                        f"\nActual: `{uploaded_data}`")


def test_blob_download(credential, container_client):
    '''Test downloading a file from blob storage'''
    container_client.get_blob_client(BLOB).upload_blob(BLOB_DATA)
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    azure.blob_download(
        account=AZURITE_URL,
        container_name=CONTAINER,
        blob_path=BLOB,
        file_path=temp_file.name,
        credential=credential,
        account_is_url=True
    )
    # Read the downloaded data
    downloaded_data = Path(temp_file.name).read_bytes()
    # Test that the downloaded data matches BLOB_DATA
    assert downloaded_data == BLOB_DATA, ("blob_download failed."
                                          f"\nExpected: `{BLOB_DATA}`"
                                          f"\nActual: `{downloaded_data}`")


def test_blob_delete(credential, container_client):
    '''Test deleting a blob'''
    # Create blob
    container_client.get_blob_client(BLOB).upload_blob(BLOB_DATA)
    container_client.get_blob_client(BLOB_2).upload_blob(BLOB_DATA)
    # Sanity check: make sure the container has one blob - the one that was just uploaded
    sc_names = sorted(list(container_client.list_blob_names()))
    exp_names = sorted([BLOB, BLOB_2])
    assert sc_names == exp_names, ("blob_delete sanity check failed:"
                                   f"\nExpected: `{exp_names}`"
                                   f"\nActual: `{sc_names}`")
    # Delete blob
    azure.blob_delete(
        account=AZURITE_URL,
        container_name=CONTAINER,
        blob_path=BLOB,
        credential=credential,
        account_is_url=True
    )
    # Test that the blob was deleted
    blob_names = list(container_client.list_blob_names())
    assert blob_names == [BLOB_2], ("blob_delete failed."
                              f"\nExpected: `{[BLOB_2]}`"
                              f"\nActual: `{blob_names}`")


def test_blob_delete_recursive(credential, container_client):
    # Create multiple blobs - 2 of which have the same prefix
    container_client.get_blob_client(BLOB).upload_blob(BLOB_DATA)
    container_client.get_blob_client(BLOB_2).upload_blob(BLOB_DATA)
    container_client.get_blob_client(BLOB_3).upload_blob(BLOB_DATA)
    # Sanity check: make sure the container has two blobs - the two that were just uploaded
    sc_names = sorted(list(container_client.list_blob_names()))
    exp_names = sorted([BLOB, BLOB_2, BLOB_3])
    assert sc_names == exp_names, ("recursive blob_delete sanity check failed:"
                                   f"\nExpected: `{exp_names}`"
                                   f"\nActual: `{sc_names}`")
    azure.blob_delete(
        account=AZURITE_URL,
        container_name=CONTAINER,
        blob_path=BLOB_DIR,
        credential=credential,
        recursive=True,
        account_is_url=True
    )
    # Test that the blobs were both deleted
    blob_names = list(container_client.list_blob_names())
    assert blob_names == [BLOB_3], ("recursive blob_delete failed."
                                    f"\nExpected: `{[BLOB_3]}`"
                                    f"\nActual: `{blob_names}`")
