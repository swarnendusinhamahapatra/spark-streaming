# assignment_4_spark_streaming.py

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Load Excel data preview for simulation
pdf = pd.read_excel("Online Retail.xlsx", nrows=5000)

pdf["InvoiceNo"] = pdf["InvoiceNo"].astype(str)
pdf["StockCode"] = pdf["StockCode"].astype(str)
pdf["CustomerID"] = pdf["CustomerID"].astype(str)
pdf["InvoiceDate"] = pd.to_datetime(pdf["InvoiceDate"])

pdf = pdf.dropna(subset=["InvoiceNo", "StockCode", "InvoiceDate"])

input_dir = "data/online_retail_stream"
os.makedirs(input_dir, exist_ok=True)

# Write data into sequential JSON files to simulate streaming batches
for i in range(0, len(pdf), 1000):
    batch = pdf.iloc[i:i+1000].copy()
    batch = pd.concat([batch, batch.head(5)])  # Introduce duplicates for testing
    batch.to_json(
        f"{input_dir}/batch_{i//1000}.json",
        orient="records", lines=True, date_format="iso"
    )

schema = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", TimestampType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", StringType()),
    StructField("Country", StringType())
])

spark = SparkSession.builder \
    .appName("OnlineRetailStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read stream from directory
stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json(input_dir)

# Apply watermarking and deduplication
clean_df = stream_df \
    .withWatermark("InvoiceDate", "1 day") \
    .dropDuplicates(["InvoiceNo", "StockCode", "InvoiceDate"])

# Write stream output to console with checkpointing
query = clean_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "data/retail_checkpoint") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("Streaming Completed Successfully")

spark.stop()