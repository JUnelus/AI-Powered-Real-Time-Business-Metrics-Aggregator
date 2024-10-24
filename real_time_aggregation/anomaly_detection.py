import pandas as pd
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get sensitive information from environment variables
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')


# Load aggregated data for anomaly detection
def load_aggregated_data(table_name):
    engine = create_engine(DB_CONNECTION_STRING)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df


# Apply anomaly detection on the aggregated data using Isolation Forest
def detect_anomalies(df):
    # Define the features for anomaly detection (e.g., avg_price, avg_change_percent)
    features = df[['avg_price', 'avg_change_percent']]

    # Initialize the Isolation Forest model
    model = IsolationForest(contamination=0.05, random_state=42)  # 5% contamination rate
    df['anomaly'] = model.fit_predict(features)

    # Label anomalies (-1 is an anomaly, 1 is normal)
    df['anomaly'] = df['anomaly'].apply(lambda x: 'anomaly' if x == -1 else 'normal')

    return df


# Save the data with anomaly labels back to the database
def save_anomaly_detection(df, table_name):
    engine = create_engine(DB_CONNECTION_STRING)
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def main():
    # Load the aggregated data
    aggregated_data = load_aggregated_data('aggregated_market_data')

    # Detect anomalies in the aggregated data
    anomaly_data = detect_anomalies(aggregated_data)

    # Save the data with anomaly labels into a new table
    save_anomaly_detection(anomaly_data, 'anomaly_detected_market_data')


if __name__ == "__main__":
    main()
