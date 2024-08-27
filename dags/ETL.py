import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
import sys
import os
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.getcwd())
from dags.ETLcode.ETL.extract import *
from dags.ETLcode.ETL.transform import (
    transform_sp500_data, calculate_daily_returns, join_fama_french_data)
from dags.ETLcode.ETL.load import upload, get_service_client_sas
import json


args = {
    'owner': "Garett",
    'depends_on_past': False,
    "start_date": days_ago(31),
    "email": "gkaube@outlook.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="SP500_ETL",
    description="",
    default_args=args,
    catchup=False,
    start_date=datetime.datetime(2024, 8, 9, 2, 20),
    schedule="@daily"
) as pipeline:
    os.makedirs("./data", exist_ok=True)
    os.makedirs("./ETL/temp", exist_ok=True)

    # Temp file to store the names of the files that have been uploaded in case the upload process
    # fails
    with open("./ETL/temp/uploaded.json", "w") as f:
        json.dump({"uploaded": []}, f)

    # Data destination folders
    unprocessed_data = "./data/unprocessed"
    processed_data = "./data/processed"
    os.makedirs(unprocessed_data, exist_ok=True)
    os.makedirs(processed_data, exist_ok=True)

    # Azure storage variables
    storage_account = Variable.get("STORAGEACCOUNTNAME")
    account_key = Variable.get("AZUREDATALAKEKEY")
    file_system = Variable.get("STORAGEFILESYSTEM")


    client = get_service_client_sas(account_name=storage_account, sas_token=account_key)
    # Navigate to the "files" folder
    file_client = client.get_file_system_client(file_system)
    # Create the destination folders
    file_client.create_directory(processed_data)
    file_client.create_directory(unprocessed_data)


    extract_sp500_data = PythonOperator(
        task_id="Extract_daily_SP500_data",
        python_callable=extract_sp500_data_daily, 
        op_kwargs={
            "out_path": unprocessed_data
        }
    )

    extract_factor_data = PythonOperator(
        task_id="Extract_Daily_Factor_Data",
        python_callable=extract_fama_french_five_factors, 
        op_kwargs={
            "out_path": unprocessed_data
        }
    )

    transform_sp500 = PythonOperator(
        task_id="Transform_SP500_Data",
        python_callable=transform_sp500_data,
        op_kwargs={
            "input_path": unprocessed_data,
            "output_path": processed_data
        }
    )

    create_returns = PythonOperator(
        task_id="Create_SP500_Returns",
        python_callable=calculate_daily_returns,
        op_kwargs={
            "input_path": processed_data,
            "output_path": processed_data
        }
    )

    merge_factor_and_sp500_data = PythonOperator(
        task_id="Merge_Factor_and_SP500_Data",
        python_callable=join_fama_french_data,
        op_kwargs={
            "sp500_input_path": processed_data,
            "factor_input_path": unprocessed_data,
            "output_path": processed_data
        }
    )

    upload_processed = PythonOperator(
        task_id = "Load_processed_files",
        python_callable=upload,
        op_kwargs= {
            "file_client": file_client,
            "parent_folder": processed_data
        }
    )

    upload_unprocessed = PythonOperator(
        task_id = "Load_unprocessed_files",
        python_callable=upload,
        op_kwargs= {
            "file_client": file_client,
            "parent_folder": unprocessed_data
        }
    )



extract_sp500_data >> transform_sp500 >> create_returns
[extract_factor_data, create_returns] >> merge_factor_and_sp500_data
[create_returns, merge_factor_and_sp500_data] >> upload_processed
upload_processed >> upload_unprocessed