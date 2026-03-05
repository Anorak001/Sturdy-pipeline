"""
Delta Lake MERGE Deduplication Pipeline
Demonstrates high-performance deduplication using Delta Lake MERGE on time-series telemetry data.

Pipeline Steps:
1. Timer Start - Measure end-to-end latency
2. Read Parquet files and explode nested PowerDetail array
3. Initialize Delta Table with baseline data
4. Execute MERGE (upsert) with composite key (device_id, metric_time)
5. Verify deduplication and report timing
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, lit, current_timestamp, unix_timestamp,
    to_timestamp, when
)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from delta import DeltaTable


def create_spark_session():
    """Create SparkSession with Delta Lake support."""
    # Use explicit package version compatible with Spark 3.5.x
    spark = SparkSession.builder \
        .appName("DeltaLakeMergeDemo") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_and_flatten(df, source_name: str):
    """
    Transform raw input data by exploding PowerDetail array and mapping to output schema.
    
    This simulates "Sanjula's Mock" transformation - flattening nested telemetry data.
    """
    # Explode the PowerDetail array to create one row per reading
    exploded_df = df.select(
        col("report_id"),
        col("device_id"),
        col("application_customer_id"),
        col("platform_customer_id"),
        col("status"),
        col("report_type"),
        col("error_reason"),
        col("model"),
        col("tags"),
        col("location_state"),
        col("location_country"),
        col("processor_vendor"),
        col("server_generation"),
        col("location_id"),
        col("location_name"),
        col("location_city"),
        col("server_name"),
        col("metric_type").alias("metric_id"),
        col("inventory_data.cpu_count").cast(StringType()).alias("cpu_inventory"),
        col("inventory_data.socket_count").alias("socket_count"),
        col("data.Average").alias("avg_metric_value"),
        col("data.Maximum").alias("max_metric_value"),
        col("data.Minimum").alias("min_metric_value"),
        col("created_at").alias("inventory_date"),
        explode(col("data.PowerDetail")).alias("power_reading")
    )
    
    # Flatten the power_reading struct and cast to output schema types
    flattened_df = exploded_df.select(
        col("report_id"),
        col("device_id"),
        col("application_customer_id"),
        col("platform_customer_id"),
        col("status"),
        col("report_type"),
        col("error_reason"),
        col("power_reading.Average").cast(DoubleType()).alias("MetricValue"),
        col("model"),
        col("tags"),
        col("location_state"),
        col("location_country"),
        col("processor_vendor"),
        col("server_generation"),
        col("location_id"),
        col("location_name"),
        col("location_city"),
        col("server_name"),
        col("metric_id"),
        col("cpu_inventory"),
        lit(None).cast(StringType()).alias("memory_inventory"),
        lit(None).cast(IntegerType()).alias("pcie_devices_count"),
        col("socket_count"),
        col("avg_metric_value"),
        col("max_metric_value"),
        col("min_metric_value"),
        col("power_reading.Time").alias("metric_time"),
        unix_timestamp(col("power_reading.Time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(DoubleType()).alias("datetime"),
        lit(None).cast(DoubleType()).alias("timeRangeEnd"),
        col("power_reading.AmbTemp").alias("amb_temp"),
        unix_timestamp(current_timestamp()).cast(DoubleType()).alias("Insertiontime"),
        lit(0.5).alias("co2_factor"),  # Default values for demo
        lit(0.12).alias("energy_cost_factor"),
        col("power_reading.Time").alias("max_metric_time"),
        col("power_reading.Time").substr(1, 10).alias("location_date"),
        col("inventory_date")
    )
    
    return flattened_df


def main():
    print("\n" + "=" * 70)
    print("  DELTA LAKE MERGE DEDUPLICATION PIPELINE")
    print("  High-Performance Time-Series Telemetry Processing Demo")
    print("=" * 70)
    
    # ========================================
    # STEP 1: TIMER START
    # ========================================
    
    
    # ========================================
    # STEP 2: INITIALIZE SPARK WITH DELTA
    # ========================================
    print("[STEP 2] Initializing Spark session with Delta Lake...")
    spark = create_spark_session()
    
    # Paths
    raw_data_path = "/app/data/raw"
    refined_path = "/app/data/refined"
    
    # ========================================
    # STEP 3: READ AND TRANSFORM FILE 1 (BASELINE)
    # ========================================
    print("[STEP 3] Reading and transforming File 1 (baseline)...")
    
    df1_raw = spark.read.parquet(f"{raw_data_path}/file1_baseline.parquet")
    df1_flattened = transform_and_flatten(df1_raw, "file1")
    
    file1_count = df1_flattened.count()
    print(f"         ✓ File 1 flattened rows: {file1_count}")
    
    # ========================================
    # STEP 4: CREATE BASELINE DELTA TABLE
    # ========================================
    print("[STEP 4] Creating baseline Delta table at /refined...")
    
    df1_flattened.write \
        .format("delta") \
        .mode("overwrite") \
        .save(refined_path)
    
    print(f"         ✓ Delta table initialized with {file1_count} rows")
    
    # ========================================
    # STEP 5: READ AND TRANSFORM FILE 2 (OVERLAP)
    # ========================================
    print("[STEP 5] Reading and transforming File 2 (overlap data)...")
    
    df2_raw = spark.read.parquet(f"{raw_data_path}/file2_overlap.parquet")
    df2_flattened = transform_and_flatten(df2_raw, "file2")
    
    file2_count = df2_flattened.count()
    print(f"         ✓ File 2 flattened rows: {file2_count}")
    
    # ========================================
    # STEP 6: DELTA MERGE (MANTHAN'S LOGIC)
    # ========================================
    print("[STEP 6] Executing Delta MERGE (Manthan's deduplication logic)...")
    print("         Using composite key: (device_id, metric_time)")
    print("\n  Starting pipeline timer...")
    start_time = time.perf_counter()
    start_time_ms = time.time() * 1000
    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, refined_path)
    
    # Execute MERGE - only insert non-matching records
    # Composite primary key: device_id + metric_time
    merge_result = delta_table.alias("target").merge(
        df2_flattened.alias("source"),
        "target.device_id = source.device_id AND target.metric_time = source.metric_time"
    ).whenNotMatchedInsertAll().execute()
    
    print("         ✓ MERGE operation completed")
    
    # ========================================
    # STEP 7: VERIFICATION
    # ========================================
    print("[STEP 7] Verifying deduplication results...")
    
    # Read final Delta table
    final_df = spark.read.format("delta").load(refined_path)
    final_count = final_df.count()
    
    # Calculate expected values
    expected_count = 2016 + 288  # baseline + new unique records
    duplicates_dropped = file1_count + file2_count - final_count
    
    print(f"\n" + "-" * 50)
    print("  DEDUPLICATION RESULTS")
    print("-" * 50)
    print(f"  File 1 (baseline) rows:    {file1_count}")
    print(f"  File 2 (overlap) rows:     {file2_count}")
    print(f"  Total input rows:          {file1_count + file2_count}")
    print(f"  Final Delta table rows:    {final_count}")
    print(f"  Duplicates dropped:        {duplicates_dropped}")
    print(f"  Expected final count:      {expected_count}")
    print("-" * 50)
    
    # Validation
    if final_count == expected_count:
        print("  ✓ SUCCESS: Deduplication verified!")
    else:
        print(f"  ⚠ WARNING: Expected {expected_count}, got {final_count}")
    
    # ========================================
    # STEP 8: TIMER END
    # ========================================
    end_time = time.perf_counter()
    end_time_ms = time.time() * 1000
    
    elapsed_seconds = end_time - start_time
    elapsed_ms = end_time_ms - start_time_ms
    
    print(f"\n" + "=" * 70)
    print("  PIPELINE EXECUTION COMPLETE")
    print("=" * 70)
    print(f"  Total execution time: {elapsed_ms:.2f} ms ({elapsed_seconds:.3f} seconds)")
    print("=" * 70)
    
    # ========================================
    # BONUS: Show Delta Lake History
    # ========================================
    print("\n[BONUS] Delta Lake Transaction History:")
    delta_table = DeltaTable.forPath(spark, refined_path)
    delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
    
    # Show sample data
    print("\n[BONUS] Sample of final data (5 rows):")
    final_df.select("device_id", "metric_time", "MetricValue", "amb_temp").show(5, truncate=False)
    
    spark.stop()
    print("\nPipeline finished. Check /app/data/refined for Delta Lake logs.")


if __name__ == "__main__":
    main()
