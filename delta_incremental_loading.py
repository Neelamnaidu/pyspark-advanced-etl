"""
Delta Lake Incremental Loading and Change Data Capture Operations
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DeltaIncrementalLoading") \
    .config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define paths
source_path = "/path/to/source/data"
checkpoint_path = "/path/to/checkpoint"
target_path = "/path/to/target/delta/table"

# Load source data as batch
source_data = spark.read.format("parquet").load(source_path)

# Define the Delta table
delta_table = DeltaTable.forPath(spark, target_path)

# Merge the new data with the existing Delta table for incremental loading
(delta_table.alias("t")
 .merge(source_data.alias("s"), "t.id = s.id")
 .whenMatchedUpdate(set={"value": "s.value"})
 .whenNotMatchedInsert(values={"id": "s.id", "value": "s.value"})
 .execute())

# Create a checkpoint for future incremental loads
spark.sql(f"CREATE OR REPLACE TABLE delta_table CHECKPOINT AS SELECT * FROM delta_table")

# Stop Spark session
spark.stop()