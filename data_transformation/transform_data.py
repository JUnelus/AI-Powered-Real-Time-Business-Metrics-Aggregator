import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get sensitive information from environment variables
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')


# Load data for transformation
def load_data(table_name):
    engine = create_engine(DB_CONNECTION_STRING)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df


# Function to clean and transform the data
def transform_data(df):
    # 1. Handle missing values
    # Replace missing numeric values with 0, text fields with "unknown"
    df = df.fillna({
        'shortName': 'unknown',
        'longName': 'unknown',
        'regularMarketPrice': 0,
        'regularMarketPreviousClose': 0,
        'regularMarketChangePercent': 0
    })

    # 2. Normalize text columns (if any)
    df['shortName'] = df['shortName'].str.lower()  # Example: Normalizing company names
    df['longName'] = df['longName'].str.lower()

    # 3. Add calculated fields (for example, calculate percentage change if not present)
    if 'regularMarketChangePercent' not in df.columns:
        df['regularMarketChangePercent'] = ((df['regularMarketPrice'] - df['regularMarketPreviousClose']) / df['regularMarketPreviousClose']) * 100

    # 4. Drop unnecessary columns (optional, depends on what you need)
    df = df.drop(['typeDisp', 'esgPopulated', 'tradeable', 'cryptoTradeable'], axis=1, errors='ignore')

    return df


# Save the transformed data back to the database
def save_transformed_data(df, table_name):
    engine = create_engine(DB_CONNECTION_STRING)
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def main():
    # Load the raw data from the trending_market_data table
    raw_data = load_data('trending_market_data')

    # Transform the data
    transformed_data = transform_data(raw_data)

    # Save the transformed data into a new table
    save_transformed_data(transformed_data, 'transformed_market_data')


if __name__ == "__main__":
    main()
