from google.cloud import bigquery

# Đường dẫn tới file key 
KEY_PATH = "/home/ittranphu/tienth/Airflow_docker/Google_bigquery_ELT/dynamic-cove-437517-n1-43e644df6d30.json"

# Tạo client BigQuery với credentials từ file key
client = bigquery.Client.from_service_account_json(KEY_PATH)

# Test bằng cách lấy danh sách dataset trong project
datasets = list(client.list_datasets())
project = client.project

if datasets:
    print(f"Datasets trong project '{project}':")
    for ds in datasets:
        print(f" - {ds.dataset_id}")
else:
    print(f"Project '{project}' không có dataset nào.")

# (Tùy chọn) chạy thử query đơn giản
QUERY = "SELECT CURRENT_DATE() as today"
df = client.query(QUERY).to_dataframe()
print("\nKết quả query:")
print(df)
