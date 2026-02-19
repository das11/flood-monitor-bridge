
import os
import sys
import logging
from typing import List

import firebase_admin
from firebase_admin import credentials, db
from influxdb_client import InfluxDBClient, WriteOptions
from dotenv import load_dotenv

# Ensure we can import from src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.main import clean_sensor_data, load_sensor_config, FIREBASE_DB_URL, INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, SERVICE_ACCOUNT_KEY

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def backfill_data():
    logger.info("Starting Backfill Process...")

    # 1. Initialize InfluxDB Client
    if not INFLUX_TOKEN:
        logger.error("INFLUX_TOKEN is missing.")
        return

    if not INFLUX_BUCKET:
        logger.error("INFLUX_BUCKET is missing.")
        return
    
    bucket = INFLUX_BUCKET
    org = INFLUX_ORG

    if not org:
        logger.error("INFLUX_ORG is missing.")
        return

    influx_client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=org,
        timeout=30000 
    )
    # Use larger batch size for backfill
    write_api = influx_client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=1000))
    logger.info("Connected to InfluxDB.")

    # 2. Initialize Firebase
    if not os.path.exists(SERVICE_ACCOUNT_KEY):
        logger.error(f"Service Account Key not found at {SERVICE_ACCOUNT_KEY}")
        return

    # Check if app is already initialized
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL
        })
    
    logger.info(f"Connected to Firebase at {FIREBASE_DB_URL}")

    # 3. Fetch All Data
    logger.info("Fetching data from Firebase (this may take a while)...")
    ref = db.reference("/")
    snapshot = ref.get()

    if not snapshot:
        logger.info("No data found in Firebase.")
        return

    # 3.5 Load sensor configs
    sensor_configs = load_sensor_config()

    points = []
    
    # 4. Process Data
    # Snapshot is likely a dict: {'floodmonitor1': {...}, 'floodmonitor2': {...}}
    if isinstance(snapshot, dict):
        for sensor_key, sensor_data in snapshot.items():
            logger.info(f"Processing sensor: {sensor_key}")
            cfg = sensor_configs.get(sensor_key)
            if isinstance(sensor_data, dict):
                for push_id, record in sensor_data.items():
                    # Construct key like path
                    key = f"/{sensor_key}/{push_id}"
                    pt = clean_sensor_data(key, record, cfg)
                    if pt:
                        points.append(pt)
            else:
                logger.warning(f"Unexpected structure for {sensor_key}")

    logger.info(f"Processed {len(points)} data points.")

    # 5. Write to InfluxDB
    if points:
        logger.info("Writing to InfluxDB...")
        write_api.write(bucket=bucket, record=points)
        write_api.close() # Ensure flush
        logger.info("Backfill Complete.")
    else:
        logger.info("No valid points to write.")

    influx_client.close()

if __name__ == "__main__":
    backfill_data()
