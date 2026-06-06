# AI-Powered Real-Time Business Metrics Aggregator

<!-- LAST_UPDATED_BADGE_START -->
![Last Updated](https://img.shields.io/badge/Last%20Updated-2026--06--06%2013%3A29%20UTC-blue)
<!-- LAST_UPDATED_BADGE_END -->


## Project Overview

This project demonstrates a pipeline for aggregating and analyzing business metrics. The system processes stock data, detects anomalies, and applies data transformation, cleaning, and aggregation techniques using Python and SQL. The project uses PostgreSQL for storing and querying business metrics.

### Key Components:

1. **Data Ingestion**: Fetching business metrics from Yahoo Finance API through RapidAPI and storing them in PostgreSQL.
2. **Data Transformation**: Cleaning, normalizing, and enriching the ingested data with additional calculated fields.
3. **Real-Time Aggregation**: Aggregating key business metrics (average, maximum, minimum prices) and calculating change percentages.
4. **Anomaly Detection**: Detecting anomalies in business metrics (although removed, this part was initially aimed to be done using Isolation Forest).

### Prerequisites

- Python 3.11
- PostgreSQL database
- RapidAPI account and API key for Yahoo Finance
- Installed Python libraries:
  - `pandas`
  - `sqlalchemy`
  - `requests`
  - `python-dotenv`

### Project Structure

```
real-time-business-metrics-aggregator/
│
├── data_ingestion/
│   ├── ingest_data.py                # Ingests stock data from Yahoo Finance API
│   ├── data_sources_config.json       # Configuration file for API keys and database connection
│
├── data_transformation/
│   ├── transform_data.py              # Cleans and transforms the raw data
│   ├── sql_queries.sql                # SQL queries to create necessary tables in PostgreSQL
│
├── real_time_aggregation/
│   ├── aggregate_metrics.py           # Aggregates business metrics (average, max, min prices)
│
├── notebooks/
│   ├── business_metrics_analysis.ipynb # Jupyter Notebook for analyzing and visualizing business metrics
│
├── automation/
│   ├── generate_daily_report.py         # Generates daily markdown summary + screenshots
│
├── .github/workflows/
│   ├── daily-metrics-update.yml         # Daily scheduled workflow to refresh README
│
├── README.md                          # Project README
├── requirements.txt                   # Python dependencies
```

### 0. Daily GitHub Automation (README + Screenshots)

This repo includes a scheduled GitHub Action that runs every day, regenerates a market summary, updates README content, and refreshes screenshot assets.

The workflow now runs a **data quality preflight** before report generation and fails early when it detects:
- Schema drift (required source columns missing)
- Freshness breach (latest market timestamp too old)
- Excessive null ratios in key numeric fields

- Workflow file: `.github/workflows/daily-metrics-update.yml`
- Report generator: `automation/generate_daily_report.py`
- Updated assets:
  - `README.md`
  - `images/daily/top_movers.png`
  - `images/daily/change_distribution.png`
  - `artifacts/daily_report.json`
  - `artifacts/data_quality_report.json`

#### Required GitHub Secret

- `RAPIDAPI_KEY`: RapidAPI key for Yahoo Finance endpoint.

#### Run manually (local)

```bash
python automation/generate_daily_report.py
```

#### Run in test mode (sample data, no API key)

```bash
python automation/generate_daily_report.py --use-sample-data
```

### 1. Data Ingestion

The `ingest_data.py` script fetches stock data from Yahoo Finance API using RapidAPI. The data is then stored in a PostgreSQL database.

- **How to run**: 
    1. Ensure PostgreSQL is running and accessible.
    2. Set up the `.env` file for environment variables (e.g., API keys, database credentials).
    3. Run the ingestion script:
    
        ```bash
        python data_ingestion/ingest_data.py
        ```

### 2. Data Transformation

The `transform_data.py` script cleans and transforms the ingested data, including handling missing values and normalizing text fields. It also calculates additional fields like percentage price change.

- **How to run**:
    1. Run the transformation script:
    
        ```bash
        python data_transformation/transform_data.py
        ```

### 3. Real-Time Aggregation

The `aggregate_metrics.py` script aggregates the transformed data, computing average, maximum, and minimum prices for each stock. It then stores the aggregated data in the PostgreSQL database.

- **How to run**:
    1. Run the aggregation script:
    
        ```bash
        python real_time_aggregation/aggregate_metrics.py
        ```

### 4. Business Metrics Analysis

The `business_metrics_analysis.ipynb` Jupyter Notebook is used for visualizing the aggregated metrics. It can be used to generate various plots showing stock prices, trends, and more.

- **How to run**:
    1. Open the notebook in Jupyter.
    2. Run each cell to generate the visualizations.
![img.png](images/img.png)
![img_1.png](images/img_1.png)
![img_2.png](images/img_2.png)
![img_3.png](images/img_3.png)
![img_4.png](images/img_4.png)

### Conclusion

This project demonstrates a full pipeline for ingesting, transforming, and analyzing business metrics using Python, SQL, and PostgreSQL. The real-time aggregation capabilities provide insights into stock market data, and the system can be easily extended to include more advanced features like anomaly detection or forecasting.
![clear_tech_workflow_business_metrics.png](images/clear_tech_workflow_business_metrics.png)

<!-- DAILY_REPORT_START -->
## Daily Automated Market Summary

- Last refresh (UTC): **2026-06-06 13:29:52**
- Tickers tracked: **20**
- Average change: **+0.99%**
- Median change: **-1.66%**
- Top gainer: **^VIX (+39.68%)**
- Top loser: **^SOX (-10.26%)**

### Top Movers

| Symbol | Name | Price | Change % |
|---|---|---:|---:|
| ^VIX | CBOE Volatility Index | 21.51 | +39.68% |
| ZEC-USD | Zcash USD | 363.20 | +19.03% |
| XLM-USD | Stellar USD | 0.20 | +6.48% |
| BTC-EUR | Bitcoin EUR | 52894.61 | +0.83% |
| BTC-USD | Bitcoin USD | 60850.89 | +0.28% |
| SPCX | Space Exploration Technologies Corp. | 135.00 | +0.00% |
| DOGE-USD | Dogecoin USD | 0.08 | -0.27% |
| XRP-USD | XRP USD | 1.09 | -0.59% |
| ADA-USD | Cardano USD | 0.16 | -0.69% |
| ^DJI | Dow Jones Industrial Average | 50866.78 | -1.35% |

### Daily Visuals

![Top Movers](images/daily/top_movers.png)

![Change Distribution](images/daily/change_distribution.png)
<!-- DAILY_REPORT_END -->