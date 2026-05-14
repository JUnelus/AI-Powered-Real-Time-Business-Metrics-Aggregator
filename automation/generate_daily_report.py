import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests

API_URL = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/get-trending-tickers"
API_HOST = "apidojo-yahoo-finance-v1.p.rapidapi.com"
REPORT_START = "<!-- DAILY_REPORT_START -->"
REPORT_END = "<!-- DAILY_REPORT_END -->"
LAST_UPDATED_BADGE_START = "<!-- LAST_UPDATED_BADGE_START -->"
LAST_UPDATED_BADGE_END = "<!-- LAST_UPDATED_BADGE_END -->"
FRESHNESS_MAX_AGE_HOURS = 36

SOURCE_REQUIRED_COLUMNS = {
    "symbol",
    "shortName",
    "regularMarketPrice",
    "regularMarketChangePercent",
    "regularMarketTime",
}

SOURCE_OPTIONAL_COLUMNS = {
    "regularMarketVolume",
    "marketCap",
}

SAMPLE_MARKET_TIME = int(datetime.now(timezone.utc).timestamp())

SAMPLE_QUOTES = [
    {"symbol": "NVDA", "shortName": "NVIDIA", "regularMarketPrice": 1098.4, "regularMarketChangePercent": 3.7, "regularMarketVolume": 32500000, "marketCap": 2690000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "AAPL", "shortName": "Apple", "regularMarketPrice": 197.2, "regularMarketChangePercent": 0.8, "regularMarketVolume": 52000000, "marketCap": 3000000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "MSFT", "shortName": "Microsoft", "regularMarketPrice": 431.3, "regularMarketChangePercent": 1.5, "regularMarketVolume": 18300000, "marketCap": 3200000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "TSLA", "shortName": "Tesla", "regularMarketPrice": 178.2, "regularMarketChangePercent": -2.6, "regularMarketVolume": 75400000, "marketCap": 567000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "AMZN", "shortName": "Amazon", "regularMarketPrice": 183.1, "regularMarketChangePercent": -0.4, "regularMarketVolume": 43600000, "marketCap": 1890000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "GOOGL", "shortName": "Alphabet", "regularMarketPrice": 173.7, "regularMarketChangePercent": 1.2, "regularMarketVolume": 24100000, "marketCap": 2140000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "META", "shortName": "Meta", "regularMarketPrice": 497.8, "regularMarketChangePercent": -1.0, "regularMarketVolume": 13600000, "marketCap": 1260000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "AMD", "shortName": "AMD", "regularMarketPrice": 160.2, "regularMarketChangePercent": 4.1, "regularMarketVolume": 60100000, "marketCap": 258000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "NFLX", "shortName": "Netflix", "regularMarketPrice": 639.3, "regularMarketChangePercent": 2.4, "regularMarketVolume": 8200000, "marketCap": 275000000000, "regularMarketTime": SAMPLE_MARKET_TIME},
    {"symbol": "PLTR", "shortName": "Palantir", "regularMarketPrice": 24.6, "regularMarketChangePercent": 5.0, "regularMarketVolume": 94300000, "marketCap": 54800000000, "regularMarketTime": SAMPLE_MARKET_TIME},
]


def fetch_trending_quotes(use_sample_data: bool) -> list[dict]:
    if use_sample_data:
        return SAMPLE_QUOTES

    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        raise RuntimeError("RAPIDAPI_KEY is required unless --use-sample-data is set.")

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": API_HOST,
    }

    response = requests.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()

    payload = response.json()
    return payload["finance"]["result"][0]["quotes"]


def build_dataframe(quotes: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(quotes)
    required_columns = [
        "symbol",
        "shortName",
        "regularMarketPrice",
        "regularMarketChangePercent",
    ]

    optional_columns = [
        "regularMarketVolume",
        "marketCap",
    ]

    for column in required_columns:
        if column not in df.columns:
            df[column] = pd.NA

    for column in optional_columns:
        if column not in df.columns:
            df[column] = 0

    df = df[required_columns + optional_columns].copy()

    numeric_columns = [
        "regularMarketPrice",
        "regularMarketChangePercent",
        "regularMarketVolume",
        "marketCap",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["shortName"] = df["shortName"].fillna("unknown")
    df = df.dropna(subset=["symbol", "regularMarketPrice", "regularMarketChangePercent"]).reset_index(drop=True)
    return df


def run_data_quality_checks(source_df: pd.DataFrame, enforce_freshness: bool) -> dict:
    missing_columns = sorted(SOURCE_REQUIRED_COLUMNS - set(source_df.columns))
    if missing_columns:
        raise RuntimeError(f"Schema drift detected. Missing required source columns: {', '.join(missing_columns)}")

    numeric_columns = ["regularMarketPrice", "regularMarketChangePercent"]
    optional_numeric_columns = ["regularMarketVolume", "marketCap"]

    # Check optional numeric columns only if they exist
    numeric_columns_to_check = numeric_columns + [col for col in optional_numeric_columns if col in source_df.columns]

    metrics: dict[str, dict[str, float | int]] = {}

    for column in numeric_columns_to_check:
        numeric_series = pd.to_numeric(source_df[column], errors="coerce")
        null_ratio = float(numeric_series.isna().mean())
        metrics[column] = {
            "null_ratio": round(null_ratio, 4),
            "row_count": int(len(source_df)),
        }
        if null_ratio > 0.2:
            raise RuntimeError(
                f"Data quality check failed for '{column}': null ratio {null_ratio:.2%} exceeds 20% threshold."
            )

    market_time = pd.to_numeric(source_df["regularMarketTime"], errors="coerce").dropna()
    if market_time.empty:
        if enforce_freshness:
            raise RuntimeError("Freshness check failed: 'regularMarketTime' has no valid timestamps.")
        latest_market_time = None
        data_age_hours = None
    else:
        max_value = float(market_time.max())
        max_seconds = max_value / 1000 if max_value > 1_000_000_000_000 else max_value
        latest_market_time_dt = datetime.fromtimestamp(max_seconds, tz=timezone.utc)
        data_age_hours = (datetime.now(timezone.utc) - latest_market_time_dt).total_seconds() / 3600
        latest_market_time = latest_market_time_dt.isoformat()

        if enforce_freshness and data_age_hours > FRESHNESS_MAX_AGE_HOURS:
            raise RuntimeError(
                f"Freshness check failed: latest market timestamp is {data_age_hours:.2f}h old (limit {FRESHNESS_MAX_AGE_HOURS}h)."
            )

    return {
        "status": "pass",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "freshness_max_age_hours": FRESHNESS_MAX_AGE_HOURS,
        "latest_market_time": latest_market_time,
        "data_age_hours": round(data_age_hours, 3) if data_age_hours is not None else None,
        "numeric_columns": metrics,
    }


def make_plots(df: pd.DataFrame, images_dir: Path) -> tuple[Path, Path]:
    images_dir.mkdir(parents=True, exist_ok=True)

    movers = df.assign(abs_change=df["regularMarketChangePercent"].abs()).nlargest(10, "abs_change")

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ["#2e7d32" if change >= 0 else "#c62828" for change in movers["regularMarketChangePercent"]]
    ax.bar(movers["symbol"], movers["regularMarketChangePercent"], color=colors)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Top 10 Absolute Movers (Trending Tickers)")
    ax.set_xlabel("Ticker")
    ax.set_ylabel("Change %")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    top_movers_path = images_dir / "top_movers.png"
    fig.savefig(top_movers_path, dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.hist(df["regularMarketChangePercent"], bins=20, color="#1565c0", edgecolor="white")
    ax.set_title("Distribution of Change % (Trending Tickers)")
    ax.set_xlabel("Change %")
    ax.set_ylabel("Count")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    distribution_path = images_dir / "change_distribution.png"
    fig.savefig(distribution_path, dpi=150)
    plt.close(fig)

    return top_movers_path, distribution_path


def format_top_movers_table(df: pd.DataFrame) -> str:
    movers = df.sort_values("regularMarketChangePercent", ascending=False).head(10).copy()
    movers["regularMarketPrice"] = movers["regularMarketPrice"].map(lambda value: f"{value:.2f}")
    movers["regularMarketChangePercent"] = movers["regularMarketChangePercent"].map(lambda value: f"{value:+.2f}%")

    rows = ["| Symbol | Name | Price | Change % |", "|---|---|---:|---:|"]
    for _, row in movers.iterrows():
        rows.append(
            f"| {row['symbol']} | {row['shortName']} | {row['regularMarketPrice']} | {row['regularMarketChangePercent']} |"
        )
    return "\n".join(rows)


def build_report_markdown(df: pd.DataFrame, timestamp: datetime) -> tuple[str, dict]:
    top_gainer = df.loc[df["regularMarketChangePercent"].idxmax()]
    top_loser = df.loc[df["regularMarketChangePercent"].idxmin()]

    market_summary = {
        "tickers_tracked": int(df.shape[0]),
        "avg_change_pct": float(df["regularMarketChangePercent"].mean()),
        "median_change_pct": float(df["regularMarketChangePercent"].median()),
        "gainers": int((df["regularMarketChangePercent"] > 0).sum()),
        "losers": int((df["regularMarketChangePercent"] < 0).sum()),
        "top_gainer": {
            "symbol": top_gainer["symbol"],
            "change_pct": float(top_gainer["regularMarketChangePercent"]),
        },
        "top_loser": {
            "symbol": top_loser["symbol"],
            "change_pct": float(top_loser["regularMarketChangePercent"]),
        },
        "generated_at": timestamp.isoformat(),
    }

    section_lines = [
        REPORT_START,
        "## Daily Automated Market Summary",
        "",
        f"- Last refresh (UTC): **{timestamp.strftime('%Y-%m-%d %H:%M:%S')}**",
        f"- Tickers tracked: **{market_summary['tickers_tracked']}**",
        f"- Average change: **{market_summary['avg_change_pct']:+.2f}%**",
        f"- Median change: **{market_summary['median_change_pct']:+.2f}%**",
        f"- Top gainer: **{market_summary['top_gainer']['symbol']} ({market_summary['top_gainer']['change_pct']:+.2f}%)**",
        f"- Top loser: **{market_summary['top_loser']['symbol']} ({market_summary['top_loser']['change_pct']:+.2f}%)**",
        "",
        "### Top Movers",
        "",
        format_top_movers_table(df),
        "",
        "### Daily Visuals",
        "",
        "![Top Movers](images/daily/top_movers.png)",
        "",
        "![Change Distribution](images/daily/change_distribution.png)",
        REPORT_END,
    ]

    return "\n".join(section_lines), market_summary


def build_last_updated_badge(timestamp: datetime) -> str:
    value = timestamp.strftime("%Y-%m-%d %H:%M UTC")
    encoded_value = value.replace("-", "--").replace(" ", "%20").replace(":", "%3A")
    return f"![Last Updated](https://img.shields.io/badge/Last%20Updated-{encoded_value}-blue)"


def update_readme(readme_path: Path, report_section: str, timestamp: datetime) -> None:
    content = readme_path.read_text(encoding="utf-8")
    badge_block = "\n".join(
        [
            LAST_UPDATED_BADGE_START,
            build_last_updated_badge(timestamp),
            LAST_UPDATED_BADGE_END,
        ]
    )

    if LAST_UPDATED_BADGE_START in content and LAST_UPDATED_BADGE_END in content:
        badge_start = content.index(LAST_UPDATED_BADGE_START)
        badge_end = content.index(LAST_UPDATED_BADGE_END) + len(LAST_UPDATED_BADGE_END)
        content = content[:badge_start] + badge_block + content[badge_end:]
    else:
        lines = content.splitlines()
        if lines:
            lines.insert(1, "")
            lines.insert(2, badge_block)
            lines.insert(3, "")
            content = "\n".join(lines)

    if REPORT_START in content and REPORT_END in content:
        report_start = content.index(REPORT_START)
        report_end = content.index(REPORT_END) + len(REPORT_END)
        new_content = content[:report_start] + report_section + content[report_end:]
    else:
        new_content = content.rstrip() + "\n\n" + report_section + "\n"

    readme_path.write_text(new_content, encoding="utf-8")


def save_report_json(report_path: Path, report_data: dict) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a daily report and update README markdown.")
    parser.add_argument("--use-sample-data", action="store_true", help="Use embedded sample data instead of API data.")
    parser.add_argument("--check-only", action="store_true", help="Run data quality checks only.")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    readme_path = repo_root / "README.md"
    images_dir = repo_root / "images" / "daily"
    report_json_path = repo_root / "artifacts" / "daily_report.json"
    quality_report_json_path = repo_root / "artifacts" / "data_quality_report.json"

    quotes = fetch_trending_quotes(use_sample_data=args.use_sample_data)
    source_df = pd.DataFrame(quotes)
    quality_report = run_data_quality_checks(source_df, enforce_freshness=not args.use_sample_data)
    save_report_json(quality_report_json_path, quality_report)

    if args.check_only:
        return

    df = build_dataframe(quotes)

    if df.empty:
        raise RuntimeError("No valid rows found in fetched data.")

    make_plots(df, images_dir)

    timestamp = datetime.now(timezone.utc)
    report_section, report_json = build_report_markdown(df, timestamp)
    update_readme(readme_path, report_section, timestamp)
    save_report_json(report_json_path, report_json)


if __name__ == "__main__":
    main()

