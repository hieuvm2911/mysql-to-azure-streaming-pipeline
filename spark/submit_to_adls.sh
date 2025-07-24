#!/bin/bash


echo "Submitting Spark job..."
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --num-executors 2 \
  --executor-memory 1g \
  --total-executor-cores 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-azure:3.3.1 \
  --conf spark.hadoop.fs.azure.account.auth.type.sqlstream.dfs.core.windows.net=SharedKey \
  --conf spark.hadoop.fs.azure.account.key.sqlstream.dfs.core.windows.net=${AZURE_STORAGE_KEY} \
  /opt/spark/jobs/kafka_to_azure.py
