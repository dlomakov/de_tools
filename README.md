# de-tools

A collection of utility tools and mini-frameworks for Data Engineering tasks within the Apache Spark and Hadoop ecosystems.\
Includes tools for analyzing Hive table structures, Parquet file metadata, monitoring resource usage of applications in YARN, and estimating HDFS directory sizes.

---

## 📦 Contents

### 1. Structure_comparator
Language: Scala\
Purpose: Compare the schema structures of two tables in a Hadoop/Spark environment.

Features:
- Launches a SparkSession and compares:
  * Hive table schemas.
  * Partitioning fields.
  * Parquet file schemas in storage.
- Great for ensuring compatibility and identifying discrepancies between table versions.
---

### 2. Parquet_analyzer
Language: Python (using PyArrow and Pandas)\
Purpose: Analyze Parquet file metadata.

Features:
- Processes a specified Parquet file and outputs detailed analysis per row group:
  - Number of row groups.
  - Total size (in bytes).
  - Average row size (in bytes).
  - Storage density.
  - Count of NULL values.
  - Cardinality (uniqueness of values).
  - Column-level analysis within each row group:
    - Compression efficiency.
    - Data type and field name.
    - Data size.
    - Contribution to memory usage within the row group.
---

### 3. Yarn_api_parser
Language: Python (integrating with Airflow and using requests)\
Purpose: Collect and load application execution statistics from the YARN API.

Features - Arflow DAG that:
1. Queries the YARN API using requests.
2. Parses application metrics:
    - Execution time.
    - Cumulative resource usage:
        - vcore-seconds.
        - memory-MB-seconds.
3. Inserts data into PostgreSQL:
    - Into a staging area (append-only mode with all new records).
    - Into a target table (latest record per application, using row_number() logic).
---

### 4. HDFS_size_estimator
Language: Python\
Purpose: Estimate the number of files and directory size in HDFS.

Features:
- Scans a specified HDFS path.
- Counts files and subdirectories within the path.
- Estimates occupied space (factoring in replication).
- Useful for analyzing storage usage.
---

## 🚀 Installation
```bash
git clone https://github.com/dlomakov/de-tools.git
