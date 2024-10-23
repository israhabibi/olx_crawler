import os
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import json
import time

# Create a log directory if it doesn't exist
LOG_DIR = "/home/isra/Project/olx_crawler/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Configure logging with TimedRotatingFileHandler
log_file_path = os.path.join(LOG_DIR, 'olx_data_fetch.log')
handler = TimedRotatingFileHandler(log_file_path, when="midnight", interval=1, backupCount=7)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Set up the logging
logging.basicConfig(level=logging.INFO, handlers=[handler])

# API URL and headers
URL = "https://www.olx.co.id/api/relevance/v4/search"
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Linux",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
}

# Database parameters
DB_PARAMS = {
    "host": "gcp.local",
    "database": "mydb",
    "user": "myuser",
    "password": "mypassword",
    "port": "5432"
}

# Retry settings
RETRY_LIMIT = 10
RETRY_DELAY = 2  # seconds

# Function definitions (as in your original code)
def now():
    """Return the current datetime."""
    return datetime.now()

def search_olx_data(location, max_pages=5):
    """Search for OLX data based on location."""
    page_num = 0
    params = {
        "facet_limit": 100,
        "location": location,
        "location_facet_limit": 20,
        "platform": "web-desktop",
        "relaxedFilters": True,
        "spellcheck": True
    }

    ads_data_list = []
    
    while page_num < max_pages:
        try:
            if page_num > 0:
                params['page'] = page_num
            
            response = requests.get(URL, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()
            ads_data_arr = data['data']
            
            if not ads_data_arr:  # Stop if no data is returned
                logging.info(f"No more data found on page {page_num}. Stopping search.")
                break
            
            for ads_data in ads_data_arr:
                useful_data = {
                    'id': ads_data['id'],
                    'title': ads_data['title'],
                    'price': json.dumps(ads_data['price']),
                    'description': json.dumps(ads_data['description']),
                    'created_at': json.dumps(ads_data['created_at']),
                    'locations_resolved': json.dumps(ads_data.get('locations_resolved', {})),
                    'main_info': json.dumps(ads_data.get('main_info', {})),
                    'artificial_boost': json.dumps(ads_data.get('artificial_boost', None)),
                    'created_at_first': json.dumps(ads_data.get('created_at_first', None)),
                    'locations': json.dumps(ads_data.get('locations', {})),
                    'location_search': location,
                    'page': page_num,
                    'inserted_at': now(),
                    'has_promotion': json.dumps(ads_data.get('has_promotion', False)),
                    'category_id': json.dumps(ads_data.get('category_id', None)),
                    'user_id': json.dumps(ads_data.get('user_id', None)),
                    'favorites': json.dumps(ads_data.get('favorites', None)),
                    'user_type': json.dumps(ads_data.get('user_type', None)),
                }
                ads_data_list.append(useful_data)
            
            page_num += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred on page {page_num}: {e}")
            break

        time.sleep(5)

    return ads_data_list

def create_engine_with_retry():
    """Create a SQLAlchemy engine with retry logic."""
    conn_str = (
        f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
        f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    )
    
    for attempt in range(RETRY_LIMIT):
        try:
            engine = create_engine(conn_str)
            # Attempt a connection to verify the engine
            with engine.connect() as conn:
                logging.info("Database connection established.")
                return engine
        except exc.SQLAlchemyError as e:
            logging.error(f"SQLAlchemy error on attempt {attempt + 1}/{RETRY_LIMIT}: {e}")
            if attempt < RETRY_LIMIT - 1:
                logging.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logging.error("Max retries exceeded. Unable to connect to the database.")
                raise

def insert_to_db(location_id, chunk_size=1000):
    """Insert the fetched OLX ads data into the PostgreSQL database in chunks."""
    ads_data_list = search_olx_data(location_id, max_pages=15)
    
    if ads_data_list:
        df = pd.DataFrame(ads_data_list)
        
        # Log the DataFrame to check its structure
        # logging.info(f"DataFrame to insert for location {location_id}:\n{df.head()}")
        
        try:
            engine = create_engine_with_retry()
            # Insert data in chunks
            for start in range(0, len(df), chunk_size):
                end = start + chunk_size
                df_chunk = df.iloc[start:end]
                df_chunk.to_sql('olx_ads_no_query', engine, if_exists='append', index=False)
                logging.info(f"Inserted {len(df_chunk)} records for location {location_id} (batch {start//chunk_size + 1}).")
                
        except exc.SQLAlchemyError as e:
            logging.error(f"SQLAlchemy error occurred while inserting data for location {location_id}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while inserting data for location {location_id}: {e}")
    else:
        logging.warning(f"No data to insert for location {location_id}.")

def main():
    """Main function to execute the script."""
    logging.info("Starting the script...")
    for location_id in range(4000001, 4000199):
        insert_to_db(location_id)
        time.sleep(5)
    

if __name__ == "__main__":
    main()
