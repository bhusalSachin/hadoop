#!/usr/bin/env bash
set -euo pipefail

LOCAL_DIR="${LOCAL_DIR:-/hero/projects/hadoop_analysis/analytics/output}"
HDFS_DIR="${HDFS_DIR:-/nepse/analytics}"

if ! command -v hdfs >/dev/null 2>&1; then
  echo "HDFS command not found. Please ensure Hadoop is installed and hdfs is on PATH."
  exit 1
fi

mkdir -p "$LOCAL_DIR"

for result_dir in \
  "$HDFS_DIR/market_summary" \
  "$HDFS_DIR/stock_performance" \
  "$HDFS_DIR/top_gainers" \
  "$HDFS_DIR/top_losers" \
  "$HDFS_DIR/most_traded" \
  "$HDFS_DIR/daily_trends" \
  "$HDFS_DIR/data_quality" \
  "$HDFS_DIR/sector_analysis"; do
  if hdfs dfs -test -e "$result_dir"; then
    local_target="$LOCAL_DIR/$(basename "$result_dir")"
    echo "Copying $result_dir to $local_target"
    rm -rf "$local_target"
    hdfs dfs -copyToLocal "$result_dir" "$LOCAL_DIR/"
  else
    echo "Skipping missing result directory: $result_dir"
  fi
done

echo "Result files copied to $LOCAL_DIR"
ls -R "$LOCAL_DIR" | head -n 100
