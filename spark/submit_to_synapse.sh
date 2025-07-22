#!/bin/bash

echo "Submitting Spark job to load fact_orders..."

# Submit the Spark job
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages \
  org.apache.hadoop:hadoop-azure:3.3.1,\
  com.microsoft.azure:azure-storage:8.6.6,\
  org.apache.hadoop:hadoop-azure-datalake:3.3.1,\
  com.microsoft.azure:azure-data-lake-store-sdk:2.3.9,\
  com.microsoft.sqlserver:mssql-jdbc:10.2.0.jre8,\
  com.microsoft.azure:spark-mssql-connector_2.12:1.2.0\
  --conf spark.hadoop.fs.azure.account.auth.type.sqlstream.dfs.core.windows.net=SharedKey \
  --conf spark.hadoop.fs.azure.account.key.sqlstream.dfs.core.windows.net=${AZURE_STORAGE_KEY} \
  /opt/spark/jobs/load_to_synapse.py
