#!/bin/bash

echo "Submitting Spark job to Azure SQL Server"
# Submit job with packages for reading from ADLS and writing to Azure SQL Server
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --num-executors 2 \
  --executor-cores 2 \
  --packages org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8,com.microsoft.azure:spark-mssql-connector_2.12:1.2.0 \
  --conf spark.hadoop.fs.azure.account.auth.type.sqlstream.dfs.core.windows.net=SharedKey \
  --conf spark.hadoop.fs.azure.account.key.sqlstream.dfs.core.windows.net=${AZURE_STORAGE_KEY} \
  /opt/spark/jobs/load_to_dwh.py

