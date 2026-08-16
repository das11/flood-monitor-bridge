"""
backfill_images.py — Full-history GCS image backfill

Unlike image_poller.py (which only scans today/yesterday on a 2-minute
cadence, by design, to keep steady-state polling cheap), this walks every
blob under a sensor's configured image prefix — regardless of date — makes
each public, and indexes it into InfluxDB as a sensor_image point.

Usage:
    uv run python src/backfill_images.py --sensors floodmonitor_dev1,floodmonitor_dev2
"""

import os
import sys
import fnmatch
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from firebase_admin import credentials, storage
import firebase_admin
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.main import (
    load_sensor_config, FIREBASE_DB_URL, FIREBASE_STORAGE_BUCKET,
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET, SERVICE_ACCOUNT_KEY,
)
from src.image_poller import _get_blob_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def _process_blob(blob, sensor_id: str, view_name: str,
                   timestamp_source: str, timestamp_regex: Optional[str],
                   timestamp_format: str) -> Optional[Point]:
    try:
        blob.make_public()
        public_url = blob.public_url
    except Exception as e:
        logger.warning(f"[{sensor_id}/{view_name}] Could not make blob public '{blob.name}': {e}")
        return None

    ts = _get_blob_timestamp(blob, timestamp_source, timestamp_regex, timestamp_format)
    basename = os.path.basename(blob.name)

    return (
        Point("sensor_image")
        .tag("sensor_id", sensor_id)
        .tag("view", view_name)
        .field("image_url", public_url)
        .field("filename", basename)
        .time(ts)
    )


def backfill_images(sensors: Optional[List[str]] = None, workers: int = 16):
    if not INFLUX_TOKEN or not INFLUX_BUCKET or not INFLUX_ORG:
        logger.error("Missing INFLUX_TOKEN / INFLUX_ORG / INFLUX_BUCKET in .env")
        return
    if not FIREBASE_STORAGE_BUCKET:
        logger.error("FIREBASE_STORAGE_BUCKET is not set in .env")
        return

    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
        firebase_admin.initialize_app(cred, {
            'databaseURL': FIREBASE_DB_URL,
            'storageBucket': FIREBASE_STORAGE_BUCKET,
        })

    gcs_bucket = storage.bucket(FIREBASE_STORAGE_BUCKET)

    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=30000)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    sensor_configs = load_sensor_config()
    target_keys = sensors if sensors else [
        sid for sid, cfg in sensor_configs.items() if cfg.get("image_paths")
    ]

    total_written = 0

    for sensor_id in target_keys:
        cfg = sensor_configs.get(sensor_id)
        if not cfg:
            logger.warning(f"Skipping {sensor_id} — not in sensor_config.yaml")
            continue

        image_paths = cfg.get("image_paths")
        if not image_paths:
            logger.warning(f"Skipping {sensor_id} — no image_paths configured")
            continue

        for view_name, view_cfg in image_paths.items():
            prefix = view_cfg.get("prefix", "").rstrip("/") + "/"
            filename_pattern = view_cfg.get("filename_pattern", "*.jpg")
            timestamp_source = view_cfg.get("timestamp_source", "blob")
            timestamp_regex = view_cfg.get("timestamp_regex")
            timestamp_format = view_cfg.get("timestamp_format", "")

            logger.info(f"[{sensor_id}/{view_name}] Listing all blobs under '{prefix}' (full history)...")
            blobs = [
                b for b in gcs_bucket.list_blobs(prefix=prefix)
                if not b.name.endswith("/")
                and fnmatch.fnmatch(os.path.basename(b.name).lower(), filename_pattern.lower())
                and (b.size or 0) > 0
            ]
            logger.info(f"[{sensor_id}/{view_name}] Found {len(blobs)} images to process.")

            points: List[Point] = []
            processed = 0

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(_process_blob, blob, sensor_id, view_name,
                                timestamp_source, timestamp_regex, timestamp_format)
                    for blob in blobs
                ]
                for future in as_completed(futures):
                    pt = future.result()
                    processed += 1
                    if pt:
                        points.append(pt)

                    if len(points) >= BATCH_SIZE:
                        write_api.write(bucket=INFLUX_BUCKET, record=points)
                        total_written += len(points)
                        logger.info(f"[{sensor_id}/{view_name}] Progress: {processed}/{len(blobs)} "
                                    f"processed, {total_written} points written so far.")
                        points = []

            if points:
                write_api.write(bucket=INFLUX_BUCKET, record=points)
                total_written += len(points)

            logger.info(f"[{sensor_id}/{view_name}] Done. {processed} images processed.")

    influx_client.close()
    logger.info(f"Image backfill complete. {total_written} total points written.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill full GCS image history into InfluxDB")
    parser.add_argument("--sensors", type=str, default=None,
                        help="Comma-separated sensor keys to backfill (default: all sensors with image_paths)")
    parser.add_argument("--workers", type=int, default=16,
                        help="Parallel worker threads for GCS make_public() calls (default: 16)")
    args = parser.parse_args()
    sensor_list = [s.strip() for s in args.sensors.split(",")] if args.sensors else None
    backfill_images(sensors=sensor_list, workers=args.workers)
