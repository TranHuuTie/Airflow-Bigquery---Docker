from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import pandas as pd
import os

# Đường dẫn tới service account key bên trong container
KEY_PATH = "/opt/airflow/keys/dynamic-cove-437517-n1-43e644df6d30.json"

PROJECT_ID = "dynamic-cove-437517-n1"
SOURCE_DATASET = "DataLake"
SOURCE_TABLE = "rawData"
TARGET_DATASET = "DataWarehouse"
TARGET_TABLE = "ProcessData"

# Schema yêu cầu
TARGET_SCHEMA = [
    bigquery.SchemaField("VendorID", "INTEGER"),
    bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("passenger_count", "FLOAT"),
    bigquery.SchemaField("trip_distance", "FLOAT"),
    bigquery.SchemaField("RatecodeID", "FLOAT"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("PULocationID", "INTEGER"),
    bigquery.SchemaField("DOLocationID", "INTEGER"),
    bigquery.SchemaField("payment_type", "INTEGER"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT"),
    bigquery.SchemaField("airport_fee", "FLOAT")
]

def test_connection():
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    datasets = list(client.list_datasets())
    print(f"Datasets trong project '{client.project}':")
    for dataset in datasets:
        print(f" - {dataset.dataset_id}")

def extract_data(ti):
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
        LIMIT 10
    """
    df = client.query(query).to_dataframe()
    ti.xcom_push(key="extracted_df", value=df.to_json())  # Push JSON lên XCom

def load_data(ti):
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    # Lấy DataFrame từ XCom
    df_json = ti.xcom_pull(key="extracted_df", task_ids="extract_data")
    df = pd.read_json(df_json)

    table_id = f"{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}"

    # Tạo bảng với schema
    table = bigquery.Table(table_id, schema=TARGET_SCHEMA)
    table = client.create_table(table, exists_ok=True)  # exists_ok để tránh lỗi nếu bảng đã tồn tại

    # Load dữ liệu
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f" Đã load {len(df)} dòng vào {table_id}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "bq_etl_dag",
    default_args=default_args,
    description="ETL từ DataLake.rawData sang DataWarehouse.ProcessData",
    schedule_interval= None, # "0 7 * * *",  # Mỗi ngày lúc 07:00
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="test_connection",
        python_callable=test_connection,
    )

    t2 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    t1 >> t2 >> t3
