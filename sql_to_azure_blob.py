import os
import pyodbc
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from the hidden .env file
load_dotenv()


def run_azure_pipeline():
    # ==========================================
    # CONFIGURATION (Loaded securely from .env)
    # ==========================================
    server = os.environ.get("SQL_SERVER")
    database = os.environ.get("SQL_DATABASE")
    azure_connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "bronze-layer"

    tables = [
        "raw_orders",
        "raw_payments",
        "raw_returns",
        "raw_shipments",
        "raw_order_items"
    ]

    # ==========================================
    # EXTRACT: Connect to SQL Server
    # ==========================================
    print(f"Connecting to SQL Server: {server}...")
    sql_conn = pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"Trusted_Connection=yes;"
    )

    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

    for table in tables:
        print(f"\nExtracting '{table}' from SQL Server...")
        df = pd.read_sql(f"SELECT * FROM dbo.{table}", sql_conn)

        # Standardize column names to uppercase and replace spaces
        df.columns = [col.upper().replace(' ', '_') for col in df.columns]

        temp_csv = f"{table}.csv"
        df.to_csv(temp_csv, index=False)
        print(f"  Rows extracted: {len(df)}")

        blob_name = f"{table}/{table}.csv"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        with open(temp_csv, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        os.remove(temp_csv)
        print(f"  Uploaded to: bronze-layer/{blob_name}")

    sql_conn.close()
    print("\n" + "=" * 50)
    print("PIPELINE SUCCESS: All 5 tables landed in Azure Blob!")
    print("=" * 50)


if __name__ == "__main__":
    run_azure_pipeline()