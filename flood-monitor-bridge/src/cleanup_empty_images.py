"""
cleanup_empty_images.py — Remove 0-byte image records from InfluxDB

Scans all sensor_image records in InfluxDB, checks if the referenced
GCS blob has size == 0, and deletes those InfluxDB records.

Usage:
    docker compose exec bridge python src/cleanup_empty_images.py
    # Dry run (default): shows what would be deleted
    docker compose exec bridge python src/cleanup_empty_images.py --apply
    # Actually deletes the records
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, storage
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.main import (
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET,
    SERVICE_ACCOUNT_KEY, FIREBASE_DB_URL, FIREBASE_STORAGE_BUCKET
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cleanup_empty_images(apply: bool = False):
    """Find and remove InfluxDB records pointing to 0-byte GCS blobs."""

    if not INFLUX_TOKEN or not INFLUX_BUCKET:
        logger.error("INFLUX_TOKEN or INFLUX_BUCKET is missing.")
        return

    if not FIREBASE_STORAGE_BUCKET:
        logger.error("FIREBASE_STORAGE_BUCKET is not set.")
        return

    # Initialize Firebase
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL,
            'storageBucket': FIREBASE_STORAGE_BUCKET
        })

    gcs_bucket = storage.bucket(FIREBASE_STORAGE_BUCKET)

    # Initialize InfluxDB
    influx_client = InfluxDBClient(
        url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG
    )
    query_api = influx_client.query_api()
    delete_api = influx_client.delete_api()

    # Query all image_url records
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -365d)
      |> filter(fn: (r) => r["_measurement"] == "sensor_image")
      |> filter(fn: (r) => r["_field"] == "image_url")
    '''

    logger.info("Querying InfluxDB for all sensor_image records...")
    tables = query_api.query(query)

    # Count total records first for progress tracking
    all_records = []
    for table in tables:
        for record in table.records:
            all_records.append(record)

    total_records = len(all_records)
    logger.info(f"Found {total_records} image records to check.")

    if total_records == 0:
        logger.info("No image records found. Nothing to clean up.")
        influx_client.close()
        return

    checked = 0
    empty_count = 0
    deleted_count = 0
    errors = 0
    PROGRESS_EVERY = 25

    for record in all_records:
            checked += 1
            url = record.get_value()
            sensor_id = record.values.get("sensor_id", "unknown")
            view = record.values.get("view", "unknown")
            ts = record.get_time()

            # Extract GCS blob path from public URL
            # URL format: https://storage.googleapis.com/BUCKET/path/to/blob.jpg
            try:
                prefix = f"https://storage.googleapis.com/{FIREBASE_STORAGE_BUCKET}/"
                if url.startswith(prefix):
                    blob_path = url[len(prefix):]
                else:
                    continue

                blob = gcs_bucket.blob(blob_path)
                blob.reload()  # Fetch metadata

                if blob.size == 0:
                    empty_count += 1
                    logger.info(
                        f"{'[DELETE]' if apply else '[DRY RUN]'} "
                        f"0-byte image: {sensor_id}/{view} — {blob_path} "
                        f"(timestamp: {ts})"
                    )

                    if apply:
                        # Delete from InfluxDB by time range (narrow 1-second window)
                        start = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                        stop_dt = ts.replace(second=ts.second + 1) if ts.second < 59 \
                            else ts.replace(minute=ts.minute + 1, second=0)
                        stop = stop_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                        delete_api.delete(
                            start=start,
                            stop=stop,
                            predicate=f'_measurement="sensor_image" '
                                      f'AND sensor_id="{sensor_id}" '
                                      f'AND view="{view}"',
                            bucket=INFLUX_BUCKET,
                            org=INFLUX_ORG
                        )
                        deleted_count += 1

            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.warning(f"Error checking blob for {url}: {e}")

            # Progress update
            if checked % PROGRESS_EVERY == 0 or checked == total_records:
                pct = (checked / total_records) * 100
                print(
                    f"\r  Progress: {checked}/{total_records} ({pct:.0f}%) "
                    f"| Empty: {empty_count} | Errors: {errors}",
                    end="", flush=True
                )

    print()  # Newline after progress bar

    mode = "APPLIED" if apply else "DRY RUN"
    logger.info(f"\n{'='*50}")
    logger.info(f"Cleanup {mode} Complete")
    logger.info(f"  Total image records: {total_records}")
    logger.info(f"  Records checked:     {checked}")
    logger.info(f"  0-byte images found: {empty_count}")
    if apply:
        logger.info(f"  Records deleted:     {deleted_count}")
    else:
        logger.info(f"  Records to delete:   {empty_count}")
        logger.info(f"  Run with --apply to delete them.")
    if errors:
        logger.info(f"  Errors:              {errors}")
    logger.info(f"{'='*50}")

    influx_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove 0-byte image records from InfluxDB"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually delete records (default is dry run)"
    )
    args = parser.parse_args()
    cleanup_empty_images(apply=args.apply)
