#!/usr/bin/env python3
"""Create a few simple charts using the Spark output CSVs."""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


OUTPUT_ROOT = Path("/hero/projects/hadoop_analysis/analytics/output").resolve()
CHART_ROOT = OUTPUT_ROOT / "charts"
CHART_ROOT.mkdir(parents=True, exist_ok=True)


def load_csv_from_dir(directory: Path) -> pd.DataFrame:
    files = sorted(directory.glob("*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(file) for file in files], ignore_index=True)


def save_plot(ax, filename: str) -> None:
    plt.tight_layout()
    plt.savefig(CHART_ROOT / filename, dpi=200)
    plt.close()


# 1. Daily trading volume chart
volume_df = load_csv_from_dir(OUTPUT_ROOT / "daily_trends")
if not volume_df.empty and {"trade_date", "total_volume"}.issubset(volume_df.columns):
    volume_df = volume_df.sort_values("trade_date")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(volume_df["trade_date"], volume_df["total_volume"], marker="o")
    ax.set_title("Daily total trading volume")
    ax.set_xlabel("Trade date")
    ax.set_ylabel("Volume")
    ax.tick_params(axis="x", rotation=45)
    save_plot(ax, "daily_volume.png")

# 2. Top 10 stocks by trading volume
stock_df = load_csv_from_dir(OUTPUT_ROOT / "most_traded")
if not stock_df.empty and {"symbol", "total_trading_volume"}.issubset(stock_df.columns):
    top10 = stock_df.head(10)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(top10["symbol"], top10["total_trading_volume"])
    ax.set_title("Top 10 stocks by trading volume")
    ax.set_xlabel("Stock symbol")
    ax.set_ylabel("Volume")
    ax.tick_params(axis="x", rotation=45)
    save_plot(ax, "top_volume.png")

# 3. Top gainers
gainers_df = load_csv_from_dir(OUTPUT_ROOT / "top_gainers")
if not gainers_df.empty and {"symbol", "percentage_change"}.issubset(gainers_df.columns):
    top_gainers = gainers_df.head(10)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(top_gainers["symbol"], top_gainers["percentage_change"])
    ax.set_title("Top gainers")
    ax.set_xlabel("Stock symbol")
    ax.set_ylabel("Percentage change (%)")
    ax.tick_params(axis="x", rotation=45)
    save_plot(ax, "top_gainers.png")

# 4. Top losers
losers_df = load_csv_from_dir(OUTPUT_ROOT / "top_losers")
if not losers_df.empty and {"symbol", "percentage_change"}.issubset(losers_df.columns):
    top_losers = losers_df.head(10)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(top_losers["symbol"], top_losers["percentage_change"])
    ax.set_title("Top losers")
    ax.set_xlabel("Stock symbol")
    ax.set_ylabel("Percentage change (%)")
    ax.tick_params(axis="x", rotation=45)
    save_plot(ax, "top_losers.png")

# 5. Sector analysis if available
sector_df = load_csv_from_dir(OUTPUT_ROOT / "sector_analysis")
if not sector_df.empty and {"sector", "average_close_price"}.issubset(sector_df.columns):
    fig, ax = plt.subplots(figsize=(10, 5))
    sector_df.plot(kind="bar", x="sector", y="average_close_price", ax=ax)
    ax.set_title("Average close price by sector")
    ax.set_xlabel("Sector")
    ax.set_ylabel("Average close price")
    ax.tick_params(axis="x", rotation=45)
    save_plot(ax, "sector_performance.png")

print(f"Charts saved to {CHART_ROOT}")
