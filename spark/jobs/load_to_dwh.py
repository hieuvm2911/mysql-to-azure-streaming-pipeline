from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import IntegerType
import os

# Load environment variables
USERNAME = os.getenv("AZURE_SQL_USERNAME")  
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

# 1. Start Spark session
spark = SparkSession.builder \
    .appName("Load Fact Orders Incrementally") \
    .getOrCreate()

# 2. Load staging order data from ADLS
df_order = spark.read.parquet("abfss://temp-storage@sqlstream.dfs.core.windows.net/cdc/")

# 3. JDBC config
jdbc_url = f"jdbc:sqlserver://de-project.database.windows.net:1433;database=ecommerce;user=CloudSA70f7a274@de-project;password={PASSWORD};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 4. Get the latest created_at from DWH
df_max = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT MAX(created_at) AS max_created_at FROM dbo.fact_orders) AS latest",
    properties=connection_properties
)

max_created_at = df_max.collect()[0]['max_created_at']

# 5. Filter only new data if max_created_at exists
df_order = df_order.withColumn("created_at_ts", to_timestamp(col("created_at")))

if max_created_at is not None:
    df_order = df_order.filter(col("created_at_ts") > max_created_at)
    print(f"Only loading rows created after {max_created_at}")
else:
    print("No existing data in DWH. Loading all rows.")

# Skip if no new data
if df_order.isEmpty():
    print("No new rows to load.")
else:
    # 6. Load dimension tables
    dim_status = spark.read.jdbc(url=jdbc_url, table="dim_order_status", properties=connection_properties)
    dim_payment = spark.read.jdbc(url=jdbc_url, table="dim_payment_method", properties=connection_properties)
    dim_date = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=connection_properties)

    # 7. Transform and join with dim_date to get time_key
    df_order = df_order.withColumn("order_date", to_date(col("created_at")))
    df_order = df_order.join(dim_date, df_order.order_date == dim_date.full_date, "left") \
        .withColumnRenamed("date_key", "time_key")

    # 8. Join with other dimension tables
    df_fact = df_order \
        .join(dim_status, df_order.status == dim_status.status_name, "left") \
        .join(dim_payment, df_order.payment_method == dim_payment.method_name, "left") \
        .select(
            col("order_id"),
            col("user_id").cast(IntegerType()).alias("user_key"),
            col("product_id").cast(IntegerType()).alias("product_key"),
            col("time_key"),
            col("quantity").cast(IntegerType()),
            col("price"),
            col("total_amount"),
            col("status_key"),
            col("payment_method_key"),
            col("created_at_ts").alias("created_at")  # Needed to compare in next run
        )

    # 9. Load to DWH
    df_fact.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", jdbc_url) \
        .option("dbtable", "dbo.fact_orders") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

