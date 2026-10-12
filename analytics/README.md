# Analytics module

This module reads the NEPSE processed CSVs from the upstream project, loads them into HDFS, and runs a simple PySpark job to produce analytic summaries.

## Files

- scripts/upload_to_hdfs.sh: creates HDFS folders and uploads processed CSVs
- scripts/download_results.sh: copies result folders back to local storage
- scripts/run_demo.sh: runs the full demo flow in one command
- scripts/visualize_charts.py: creates a few simple charts from the CSV results
- spark/config.py: runtime configuration for source data and HDFS paths
- spark/nepse_analytics.py: main Spark job

## Source data paths

The job expects processed CSVs under:

```text
/hero/projects/nepse-data/data/processed
```

The upload script copies them to HDFS under:

```text
/nepse/processed
```

The analytics output is written to:

```text
/nepse/analytics
```

## Typical usage

```sh
cd /hero/projects/hadoop_analysis
./analytics/scripts/upload_to_hdfs.sh
spark-submit --master local[*] ./analytics/spark/nepse_analytics.py
./analytics/scripts/download_results.sh
python ./analytics/scripts/visualize_charts.py
```

## Important note

The analytics implementation intentionally uses the validated cleaned CSVs produced by the existing scraper pipeline. This avoids rewriting the ETL logic and keeps the project focused on a local demonstration of HDFS + Spark.
