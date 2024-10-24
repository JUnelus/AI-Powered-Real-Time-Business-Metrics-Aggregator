import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get sensitive information from environment variables
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')


# Function to ingest trending data from RapidAPI (Yahoo Finance)
def ingest_trending_data():
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/get-trending-tickers"
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': 'apidojo-yahoo-finance-v1.p.rapidapi.com'
    }

    response = requests.get(url, headers=headers)

    # Log the response details for debugging
    print(f"Response status code: {response.status_code}")
    print(f"Response text: {response.text}")  # Log response content

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from Yahoo Finance API. Status code: {response.status_code}")


# Function to save data into PostgreSQL database
def save_data_to_db(data, table_name):
    engine = create_engine(DB_CONNECTION_STRING)

    # Extract the correct part of the response based on the actual structure
    trending_data = data['finance']['result'][0]['quotes']  # Access the 'quotes' key within 'result'

    # Convert the data into a DataFrame
    df = pd.DataFrame(trending_data)

    # Save DataFrame to PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)


# Main function to orchestrate data ingestion
def main():
    # Fetch trending data from RapidAPI (Yahoo Finance)
    data = ingest_trending_data()

    # Save trending data to the database
    save_data_to_db(data, 'trending_market_data')


if __name__ == "__main__":
    main()
