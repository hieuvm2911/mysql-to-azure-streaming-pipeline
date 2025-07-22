from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Define schema matching the Debezium payload
value_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

# Create Spark session with configs for ADLS Gen2
spark = SparkSession.builder \
    .appName("KafkaToAzureADLS") \
    .getOrCreate()

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce.ecommerce.orders") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka message value is in JSON string format
df_values = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

# Parse the JSON value
df_parsed = df_values.select(from_json(col("json_str"), value_schema).alias("data")) \
    .select("data.*")

# Optional: convert fields to correct types (String → Int/Double/Timestamp)
df_cleaned = df_parsed.select(
    col("order_id").cast("long"),
    col("user_id").cast("int"),
    col("product_id").cast("int"),
    col("quantity").cast("int"),
    col("price").cast("double"),
    col("total_amount").cast("double"),
    col("status"),
    col("payment_method"),
    (col("created_at") / 1000).cast("timestamp").alias("created_at"),
    (col("updated_at") / 1000).cast("timestamp").alias("updated_at")
)

# Define ADLS Gen2 output path
output_path = "abfss://temp-storage@sqlstream.dfs.core.windows.net/cdc/"

# Write to ADLS Gen2 in Parquet format
query = df_cleaned.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "/tmp/checkpoint_orders") \
    .outputMode("append") \
    .start()

query.awaitTermination()
