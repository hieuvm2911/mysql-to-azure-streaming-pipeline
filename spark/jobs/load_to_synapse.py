from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, concat_ws
from pyspark.sql.types import IntegerType

import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")
# Load environment variables
SYSNAPSE_USERNAME = os.getenv("SYSNAPSE_USERNAME")
SYSNAPSE_PASSWORD = os.getenv("SYSNAPSE_PASSWORD")

# 1. Spark session
spark = SparkSession.builder \
    .appName("Load Fact Orders") \
    .getOrCreate()

# 2. Load staging order data (from parquet or Kafka sink to ADLS)
df_order = spark.read.parquet("abfss://temp-storage@sqlstream.dfs.core.windows.net/cdc/")

# 3. Load dimension tables
jdbc_url = "jdbc:sqlserver://mysql-cdc-stream.sql.azuresynapse.net:1433;database=dwh"
connection_properties = {
    "user": SYSNAPSE_USERNAME,
    "password": SYSNAPSE_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

dim_status = spark.read.jdbc(url=jdbc_url, table="dim_order_status", properties=connection_properties)
dim_payment = spark.read.jdbc(url=jdbc_url, table="dim_payment_method", properties=connection_properties)
dim_date = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=connection_properties)


# 4. Create time_key by matching order creation date
df_order = df_order.withColumn("order_date", to_date(col("created_at")))

# 5. Join with dim_date to get time_key
df_order = df_order.join(dim_date, df_order.order_date == dim_date.full_date, "left") \
    .withColumnRenamed("date_key", "time_key")

# 6. Join with dimension tables to map surrogate keys
df_fact = df_order \
    .join(dim_status, df_order.status == dim_status.status_name, "left") \
    .join(dim_payment, df_order.payment_method == dim_payment.method_name, "left") \
    .select(
        col("order_id"),
        col("user_key"),
        col("product_key"),
        col("time_key"),
        col("quantity").cast(IntegerType()),
        col("price"),
        col("total_amount"),
        col("status_key"),
        col("payment_method_key")
    )

# 7. Write fact_orders (Synapse)
df_fact.write \
  .format("com.microsoft.spark.sqlanalytics") \
  .mode("append") \
  .option("url", "jdbc:sqlserver://<server>.sql.azuresynapse.net:1433;database=dwh;encrypt=true;trustServerCertificate=false") \
  .option("dbtable", "dbo.fact_orders") \
  .option("tempDir", "abfss://temp-storage@sqladmindatalake.dfs.core.windows.net/synapse/tmp/") \
  .option("authentication", "ActiveDirectoryManagedIdentity") \
  .save()

