import pandas as pd
import requests
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from datetime import datetime

# Database setup
engine = create_engine('sqlite:///database.db')
metadata = MetaData()

# Define table schema
users = Table('users', metadata,
              Column('id', Integer, primary_key=True, autoincrement=True),
              Column('name', String),
              Column('age', Integer, nullable=True),
              Column('city', String),
              Column('source', String)
              )

metadata.create_all(engine)


def log_message(message):
    """Log messages to a log file."""
    with open("etl_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")


def extract_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        log_message("CSV data extracted successfully.")
        return df
    except Exception as e:
        log_message(f"Error extracting CSV: {e}")
        return pd.DataFrame()


def extract_json(file_path):
    try:
        df = pd.read_json(file_path)
        log_message("JSON data extracted successfully.")
        return df
    except Exception as e:
        log_message(f"Error extracting JSON: {e}")
        return pd.DataFrame()


def extract_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data)
        log_message("API data extracted successfully.")
        return df
    except Exception as e:
        log_message(f"Error extracting API: {e}")
        return pd.DataFrame()


def transform_data(df, source):
    """Standardize column names and format."""
    if df.empty:
        return df

    if source == 'csv':
        df = df.rename(columns={'name': 'name', 'age': 'age', 'city': 'city'})
        df['source'] = 'csv'
        df = df[['name', 'age', 'city', 'source']]

    elif source == 'json':
        df = df.rename(columns={'full_name': 'name', 'location': 'city'})
        df['age'] = None
        df['source'] = 'json'
        df = df[['name', 'age', 'city', 'source']]

    elif source == 'api':
        df = df.rename(columns={'name': 'name', 'address.city': 'city'})
        df['age'] = None
        df['source'] = 'api'
        df = df[['name', 'age', 'city', 'source']]

    log_message(f"{source.upper()} data transformed successfully.")
    return df


def load_data(df):
    """Load DataFrame into the database."""
    if df.empty:
        log_message("No data to load.")
        return

    try:
        df.to_sql('users', con=engine, if_exists='append', index=False)
        log_message(f"Loaded {len(df)} records into the database.")
    except Exception as e:
        log_message(f"Error loading data: {e}")


def run_etl():
    log_message("ETL Job Started.")

    # Extract
    csv_data = extract_csv('data/sample_data.csv')
    json_data = extract_json('data/sample_data.json')
    api_data = extract_api('https://jsonplaceholder.typicode.com/users')

    # Transform
    csv_data = transform_data(csv_data, 'csv')
    json_data = transform_data(json_data, 'json')
    api_data = transform_data(api_data, 'api')

    # Load
    load_data(csv_data)
    load_data(json_data)
    load_data(api_data)

    log_message("ETL Job Completed.\n")


if __name__ == "__main__":
    run_etl()
