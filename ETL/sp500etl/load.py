import os

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
import logging
from tqdm import tqdm
import json

logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)
logging.getLogger('azure.storage').setLevel(logging.ERROR)
logger2 = logging.getLogger('azure')
logger2.setLevel(logging.ERROR)

logger = logging.getLogger("etl")

def get_service_client_sas(account_name: str, sas_token: str) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    # The SAS token string can be passed in as credential param or appended to the account URL
    service_client = DataLakeServiceClient(account_url, credential=sas_token)
    return service_client


def upload_file_to_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)


def upload(file_client, parent_folder):
    import glob

    client = file_client.get_directory_client("data/unprocessed") if "unprocessed" in parent_folder else file_client.get_directory_client("data/processed")

    n = len(glob.glob(f"{parent_folder}/*/*.parquet"))
    progress_bar = tqdm(total=n, desc=f"Uploading data from {parent_folder}")

    with open("./ETL/temp/uploaded.json", "r") as f:
        uploaded_list = json.load(f)['uploaded']

    for folder in os.listdir(parent_folder):
        file_client.create_directory(f"{parent_folder}/{folder}")

        for file in os.listdir(f"{parent_folder}/{folder}"):
            upload_file_to_directory(client, f"{parent_folder}/{folder}", file)
            uploaded_list.append(f"{parent_folder}/{folder}/{file}")

            with open("./ETL/temp/uploaded.json", "w") as f:
                json.dump({"uploaded": uploaded_list}, f)

            progress_bar.update(1)





def main():
    pass


if __name__ == "__main__":
    main()