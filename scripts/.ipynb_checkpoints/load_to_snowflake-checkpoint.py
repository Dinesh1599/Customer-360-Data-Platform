import pandas as pd
import json
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

# Load config
with open("../config/snowflake_config.json") as f:
    creds = json.load(f)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=creds["user"],
    password=creds["password"],
    account=creds["account"],
    warehouse=creds["warehouse"],
    database=creds["database"],
    schema=creds["schema"],
    role=creds["role"]
)

cs = conn.cursor()

# Create table if it doesn’t exist
cs.execute("""
    CREATE TABLE IF NOT EXISTS FACT_LTV (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        ltv FLOAT
    )
""")

# Read parquet into DataFrame
df = pd.read_parquet("../staging/fact_ltv.parquet")


# Before write_pandas
df.columns = [col.upper() for col in df.columns]
# Upload
success, _, num_chunks, num_rows, _ = write_pandas(conn, df, 'FACT_LTV')
print(f"Upload success: {success}, Rows: {num_rows}")

cs.close()
conn.close()
