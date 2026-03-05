# Sturdy Pipeline

An experimental data pipeline that ingests streaming data from Kafka, processes it with Spark Structured Streaming, and stores results in PostgreSQL. This project serves no purpose, feel free to fork and build something useful on top of it.

---

## Delta Lake MERGE Deduplication Demo

A Dockerized demonstration of high-performance deduplication using Delta Lake MERGE on time-series telemetry data.

### Overview

This demo showcases:
- **Nested data flattening** using PySpark's `explode()` function
- **Delta Lake MERGE** (upsert) operations with composite primary keys
- **Deduplication** of overlapping time-series telemetry data
- **Performance measurement** of end-to-end pipeline execution

### The Scenario

| File | Device | PowerDetail Readings | Time Coverage |
|------|--------|---------------------|---------------|
| File 1 (Baseline) | SRV-101 | 2016 | Days 1-7 |
| File 2 (Overlap) | SRV-101 | 2016 | Days 2-8 |

- **Overlap**: 1728 readings (Days 2-7) share the same timestamps
- **New data**: 288 readings (Day 8) are unique to File 2
- **Expected result**: 2016 + 288 = **2304 rows** after MERGE (1728 duplicates dropped)

### Quick Start

#### Prerequisites
- Docker Desktop
- Docker Compose

#### Run the Full Demo

```bash
# Build and run the complete demo
docker-compose up --build
```

This will:
1. Generate two Parquet files with overlapping telemetry data
2. Flatten the nested `PowerDetail` arrays
3. Create a baseline Delta table
4. Execute MERGE to deduplicate
5. Verify the final row count (2304)
6. Report execution time in milliseconds

#### Run Individual Steps

```bash
# Build the container
docker-compose build

# Generate test data only
docker-compose run spark python generate_data.py

# Run the deduplication pipeline only
docker-compose run spark python delta_merge_pipeline.py
```

#### Interactive Shell

```bash
# Start a bash shell in the container
docker-compose run spark bash

# Then run commands interactively
python generate_data.py
python delta_merge_pipeline.py
```

#### Jupyter Notebook (Optional)

```bash
# Start with Jupyter profile
docker-compose --profile jupyter up jupyter

# Open http://localhost:8888 in your browser
```

### Inspect Delta Lake Files

After running the demo, you can explore the Delta Lake files on your host machine:

```
./data/
├── raw/
│   ├── file1_baseline.parquet/
│   └── file2_overlap.parquet/
└── refined/
    ├── _delta_log/           # Transaction log
    │   ├── 00000000000000000000.json
    │   └── 00000000000000000001.json
    └── *.parquet             # Data files
```

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA GENERATION                             │
├─────────────────────────────────────────────────────────────────┤
│  File 1: SRV-101 with 2016 PowerDetail readings (Days 1-7)     │
│  File 2: SRV-101 with 2016 PowerDetail readings (Days 2-8)     │
│          - 1728 overlap (Days 2-7)                              │
│          - 288 new (Day 8)                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     TRANSFORMATION                              │
├─────────────────────────────────────────────────────────────────┤
│  1. Read Parquet files                                          │
│  2. explode(PowerDetail) → One row per reading                  │
│  3. Cast/transform to output_schema                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DELTA LAKE MERGE                            │
├─────────────────────────────────────────────────────────────────┤
│  Composite Key: (device_id, metric_time)                        │
│                                                                 │
│  MERGE INTO refined AS target                                   │
│  USING file2_data AS source                                     │
│  ON target.device_id = source.device_id                         │
│     AND target.metric_time = source.metric_time                 │
│  WHEN NOT MATCHED THEN INSERT *                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     VERIFICATION                                │
├─────────────────────────────────────────────────────────────────┤
│  Final count: 2304 rows                                         │
│  - 2016 from baseline                                           │
│  - 288 new from overlap file                                    │
│  - 1728 duplicates dropped ✓                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Expected Output

```
======================================================================
  DELTA LAKE MERGE DEDUPLICATION PIPELINE
  High-Performance Time-Series Telemetry Processing Demo
======================================================================

[STEP 1] Starting pipeline timer...
[STEP 2] Initializing Spark session with Delta Lake...
[STEP 3] Reading and transforming File 1 (baseline)...
         ✓ File 1 flattened rows: 2016
[STEP 4] Creating baseline Delta table at /refined...
         ✓ Delta table initialized with 2016 rows
[STEP 5] Reading and transforming File 2 (overlap data)...
         ✓ File 2 flattened rows: 2016
[STEP 6] Executing Delta MERGE (Manthan's deduplication logic)...
         Using composite key: (device_id, metric_time)
         ✓ MERGE operation completed
[STEP 7] Verifying deduplication results...

--------------------------------------------------
  DEDUPLICATION RESULTS
--------------------------------------------------
  File 1 (baseline) rows:    2016
  File 2 (overlap) rows:     2016
  Total input rows:          4032
  Final Delta table rows:    2304
  Duplicates dropped:        1728
  Expected final count:      2304
--------------------------------------------------
  ✓ SUCCESS: Deduplication verified!

======================================================================
  PIPELINE EXECUTION COMPLETE
======================================================================
  Total execution time: XXXX.XX ms (X.XXX seconds)
======================================================================
```

### Files

| File | Description |
|------|-------------|
| `generate_data.py` | Creates test Parquet files with overlapping timestamps |
| `delta_merge_pipeline.py` | PySpark script with Delta Lake MERGE logic |
| `input_schema.py` | Input schema with nested PowerDetail array |
| `output_schema.py` | Flattened output schema |
| `Dockerfile` | Container image with PySpark + Delta Lake |
| `docker-compose.yml` | Container orchestration |

### Cleanup

```bash
# Stop containers
docker-compose down

# Remove generated data
rm -rf ./data

# Remove Docker images
docker-compose down --rmi all
```
