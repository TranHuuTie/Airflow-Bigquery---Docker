from google.cloud import bigquery

KEY_PATH = "/home/ittranphu/tienth/Airflow_docker/Google_bigquery_ELT/dynamic-cove-437517-n1-43e644df6d30.json"

client = bigquery.Client.from_service_account_json(KEY_PATH)

dataset_id = f"{client.project}.DataLake"

tables = list(client.list_tables(dataset_id))

if tables:
    print(f"Dataset '{dataset_id}' có {len(tables)} bảng:")
    for table in tables:
        print(f" - {table.table_id}")
else:
    print(f"Dataset '{dataset_id}' không có bảng nào.")

# Query 10 dòng từ bảng rawData
query = """
    SELECT *
    FROM `dynamic-cove-437517-n1.DataLake.rawData`
    LIMIT 10
"""

df = client.query(query).to_dataframe()

print("\n Dữ liệu:")
print(df)

print("\n Schema của DataFrame:")
for col, dtype in zip(df.columns, df.dtypes):
    print(f" - {col}: {dtype}")
