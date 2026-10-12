#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${SOURCE_DIR:-/hero/projects/nepse-data/data/processed}"
HDFS_BASE="${HDFS_BASE:-/nepse}"

if ! command -v hdfs >/dev/null 2>&1; then
  echo "HDFS command not found. Please ensure Hadoop is installed and hdfs is on PATH before using HDFS storage."
  echo "This environment is using the local processed CSV path for demo execution."
  exit 0
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "Source directory not found: $SOURCE_DIR"
  exit 1
fi

echo "Creating HDFS folders under $HDFS_BASE"
hdfs dfs -mkdir -p "$HDFS_BASE/raw" "$HDFS_BASE/processed" "$HDFS_BASE/analytics"

echo "Uploading processed NEPSE CSV files from $SOURCE_DIR"
while IFS= read -r file; do
  relative_path="${file#$SOURCE_DIR/}"
  target_path="$HDFS_BASE/processed/$relative_path"
  hdfs dfs -mkdir -p "$(dirname "$target_path")"
  hdfs dfs -put -f "$file" "$target_path"
  echo "Uploaded: $file -> $target_path"
done < <(find "$SOURCE_DIR" -type f \( -name "today_share_price.csv" -o -name "index_history.csv" -o -name "cleaning_report.json" \) | sort)

echo "HDFS verification"
hdfs dfs -ls -R "$HDFS_BASE/processed" | head -n 50

echo "Upload complete. Processed files are available under $HDFS_BASE/processed"
