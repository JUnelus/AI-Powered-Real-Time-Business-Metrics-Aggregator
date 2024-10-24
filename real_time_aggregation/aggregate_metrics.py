import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get sensitive information from environment variables
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')


# Load transformed data for aggregation
def load_transformed_data(table_name):
    engine = create_engine(DB_CONNECTION_STRING)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df


# Perform real-time aggregation on the data
def aggregate_metrics(df):
    # Group by 'symbol' (company ticker) and calculate aggregated metrics
    aggregated_df = df.groupby('symbol').agg(
        avg_price=('regularMarketPrice', 'mean'),
        max_price=('regularMarketPrice', 'max'),
        min_price=('regularMarketPrice', 'min'),
        avg_change_percent=('regularMarketChangePercent', 'mean')
    ).reset_index()

    return aggregated_df


# Save aggregated data to the database
def save_aggregated_data(df, table_name):
    engine = create_engine(DB_CONNECTION_STRING)
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def main():
    # Load the transformed data
    transformed_data = load_transformed_data('transformed_market_data')

    # Perform aggregation on the data
    aggregated_data = aggregate_metrics(transformed_data)

    # Save the aggregated data into a new table
    save_aggregated_data(aggregated_data, 'aggregated_market_data')


if __name__ == "__main__":
    main()
