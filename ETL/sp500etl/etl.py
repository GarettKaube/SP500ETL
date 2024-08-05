from extract import *
from transform import *
from load import *
import logging
import json
from tqdm import tqdm
import warnings
import time
import os

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)
logging.getLogger('azure.mgmt.resource').setLevel(logging.WARNING)
logging.getLogger('azure.storage').setLevel(logging.WARNING)

logger2 = logging.getLogger('azure')
logger2.setLevel(logging.ERROR)

class ETL:
    def __init__(self, unprocessed_path, processed_path) -> None:
        # Azure data lake gen 2 config
        with open("./config/datalake.json", "r") as f:
            datalake_conf = json.load(f)
            self.dl_account = datalake_conf['account_name']
            key = os.environ["AZUREDATALAKE"]
            self.file_system = datalake_conf["file_system"]

        os.makedirs("ETL/temp", exist_ok=True)
        with open("./ETL//temp/uploaded.json", "w") as f:
            json.dump({"uploaded": []}, f)
        
        self.processed_path = processed_path
        self.unprocessed_path = unprocessed_path

        self.retry_time = 30

        # Make the local destination folders
        os.makedirs(unprocessed_path, exist_ok=True)
        os.makedirs(processed_path, exist_ok=True)

        self.client = get_service_client_sas(account_name=self.dl_account, sas_token=key)
        # Navigate to the "files" folder
        self.file_client = self.client.get_file_system_client("files")
        # Create the destination folders
        self.file_client.create_directory(self.processed_path)
        self.file_client.create_directory(self.unprocessed_path)
    

    def extract(self, retrys=1):
        try:
            logger.info(f"Starting {self.extract.__name__} process")
            logger.info(f"Extracting S&P500 data..")
            extract_sp500_data_daily(self.unprocessed_path)
            logger.info(f"Extracting Fama-French Five Factor data..")
            extract_fama_french_five_factors(self.unprocessed_path)
            logger.info("Extract complete")
        except Exception as e:
            logger.exception(f"Extract failed, retrys: {retrys}", exc_info=True)
            # Retry the process after self.retry_time seconds
            if retrys > 0:
                logger.info(f"Retrying {self.extract.__name__} in {self.retry_time} seconds")
                time.sleep(self.retry_time)
                self.extract(retrys=retrys-1)
        return self


    def transform(self):
        logger.info(f"Starting {self.transform.__name__} process")
        logger.info("Creating cumulative return data..")
        calculate_cum_return(self.unprocessed_path, self.processed_path)
        logger.info("Done")
        logger.info("Reformatting S&500 data..")
        transform_sp500_data(self.unprocessed_path, self.processed_path)
        logger.info("Creating Joined S&P500 Factor data..")
        join_fama_french_data(
            sp500_input_path=self.processed_path, 
            factor_input_path=self.unprocessed_path, 
            output_path=self.processed_path
        )
        logger.info("Done")
        return self

    
    def load(self, retrys=1):
        logger.info(f"Starting {self.load.__name__} process")
        logger.info(f"""Saving data to Datalake account {self.dl_account} to folders 
                    {self.file_system}/{self.processed_path} 
                    and {self.file_system}/{self.unprocessed_path}""")
        
        for parent_folder in [self.processed_path, self.unprocessed_path]:
            try:
                upload(self.file_client, parent_folder)
            except Exception as e:
                logger.exception(f"{self.extract.__name__} failed", exc_info=True)
                if retrys > 0:
                    logger.info(f"Retrying {self.load.__name__} in {self.retry_time} seconds")
                    time.sleep(self.retry_time)
                    self.load(retrys=retrys-1)
                else:
                    logger.error(f"{self.load.__name__} process failed")
            else:
                with open("./temp/uploaded.json", "w") as f:
                    json.dump({"uploaded": []}, f)


        logger.info(f"Upload to {self.dl_account} Complete")
        return self


def main():
    try:
        from localsetup import set_datalake_creds
        set_datalake_creds()
    except Exception as e:
        pass

    unprocessed_data = "./data/unprocessed"
    processed_data = "./data/processed"

    pipeline = ETL(unprocessed_data, processed_data)
    pipeline.extract()\
        .transform()\
        #.load()
    logger.info("ETL complete")


if __name__ == "__main__":
    main()
