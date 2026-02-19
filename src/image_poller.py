"""
image_poller.py — Firebase Storage Image Poller

Polls Firebase Storage for new sensor camera images, makes them public,
and writes image URL metadata into InfluxDB for time-linked display
in Grafana and the SPA dashboard.

Each sensor can define unique image paths and naming conventions
in sensor_config.yaml under the `image_paths` key.
"""

import os
import re
import time
import logging
import datetime
import fnmatch
import threading
from typing import Dict, Any, Optional

from firebase_admin import storage
from influxdb_client import InfluxDBClient, Point, WriteOptions

logger = logging.getLogger(__name__)

# ── State: tracks which blobs have already been ingested ─────
_seen_blobs: Dict[str, set] = {}  # {sensor_id/view: set(blob_names)}


def _extract_timestamp_from_filename(
    filename: str,
    regex_pattern: str,
    timestamp_format: str = ""
) -> Optional[datetime.datetime]:
    """
    Extract a datetime from a filename using a regex pattern.
    
    If timestamp_format is 'epoch_s' or 'epoch_ms', the first capture group
    is treated as an epoch timestamp (e.g. '1771511518_front.jpg').
    
    Otherwise, capture groups are interpreted as datetime components
    (YYYY, MM, DD, HH, MM, SS).
    """
    match = re.search(regex_pattern, filename)
    if not match:
        return None

    groups = match.groups()
    if not groups:
        return None

    try:
        # Epoch-based timestamp
        if timestamp_format in ("epoch_s", "epoch_ms"):
            epoch_val = float(groups[0])
            if timestamp_format == "epoch_ms":
                epoch_val = epoch_val / 1000.0
            return datetime.datetime.fromtimestamp(
                epoch_val, tz=datetime.timezone.utc
            )

        # Multi-group datetime parsing
        if len(groups) >= 6:
            dt = datetime.datetime(
                int(groups[0]), int(groups[1]), int(groups[2]),
                int(groups[3]), int(groups[4]), int(groups[5]),
                tzinfo=datetime.timezone.utc
            )
        elif len(groups) >= 3:
            vals = [int(g) for g in groups]
            if vals[0] > 100:  # Looks like a year
                dt = datetime.datetime(
                    vals[0], vals[1], vals[2],
                    tzinfo=datetime.timezone.utc
                )
            else:
                today = datetime.date.today()
                dt = datetime.datetime(
                    today.year, today.month, today.day,
                    vals[0], vals[1], vals[2],
                    tzinfo=datetime.timezone.utc
                )
        else:
            # Single group but not epoch — try as epoch_s anyway
            epoch_val = float(groups[0])
            if epoch_val > 1_000_000_000:  # Looks like an epoch
                return datetime.datetime.fromtimestamp(
                    epoch_val, tz=datetime.timezone.utc
                )
            return None
        return dt
    except (ValueError, IndexError, OSError, OverflowError):
        return None


def _get_blob_timestamp(
    blob: Any,
    timestamp_source: str,
    timestamp_regex: Optional[str] = None,
    timestamp_format: str = ""
) -> datetime.datetime:
    """
    Get the timestamp from a blob, using either its GCS metadata
    or by parsing its filename.
    """
    if timestamp_source == "filename" and timestamp_regex:
        filename = os.path.basename(blob.name)
        ts = _extract_timestamp_from_filename(
            filename, timestamp_regex, timestamp_format
        )
        if ts:
            return ts
        logger.warning(
            f"Could not extract timestamp from filename '{filename}' "
            f"with regex '{timestamp_regex}', falling back to blob metadata"
        )

    # Default: use blob's creation time
    if blob.time_created:
        return blob.time_created.replace(tzinfo=datetime.timezone.utc)

    return datetime.datetime.now(datetime.timezone.utc)


def _build_prefix_for_date(
    base_prefix: str,
    target_date: datetime.date,
    use_date_folders: bool
) -> str:
    """
    Build the full GCS prefix path.
    If date_folders=true, appends YYYY-MM-DD/.
    If flat, just use the base prefix.
    """
    prefix = base_prefix.rstrip("/")
    if use_date_folders:
        prefix = f"{prefix}/{target_date.isoformat()}"
    return prefix + "/"


def poll_sensor_images(
    sensor_configs: Dict[str, Dict[str, Any]],
    write_api: Any,
    bucket_name: str,
    influx_bucket: str,
    poll_interval: int = 120
):
    """
    Main polling loop for sensor images.
    Runs indefinitely, checking for new images every `poll_interval` seconds.
    """
    logger.info(
        f"Image poller starting (interval={poll_interval}s, "
        f"storage_bucket={bucket_name})"
    )

    # Initialize the storage bucket
    try:
        gcs_bucket = storage.bucket(bucket_name)
    except Exception as e:
        logger.error(f"Failed to access Firebase Storage bucket '{bucket_name}': {e}")
        return

    while True:
        try:
            _poll_once(sensor_configs, write_api, gcs_bucket, influx_bucket)
        except Exception as e:
            logger.error(f"Image poller error: {e}", exc_info=True)

        time.sleep(poll_interval)


def _poll_once(
    sensor_configs: Dict[str, Dict[str, Any]],
    write_api: Any,
    gcs_bucket: Any,
    influx_bucket: str
):
    """Single polling iteration across all sensors and views."""
    for sensor_id, cfg in sensor_configs.items():
        image_paths = cfg.get("image_paths")
        if not image_paths:
            continue

        for view_name, view_cfg in image_paths.items():
            prefix = view_cfg.get("prefix", "")
            date_folders = view_cfg.get("date_folders", True)
            filename_pattern = view_cfg.get("filename_pattern", "*.jpg")
            timestamp_source = view_cfg.get("timestamp_source", "blob")
            timestamp_regex = view_cfg.get("timestamp_regex")
            timestamp_format = view_cfg.get("timestamp_format", "")

            # Build the tracking key
            track_key = f"{sensor_id}/{view_name}"
            if track_key not in _seen_blobs:
                _seen_blobs[track_key] = set()

            # Determine which prefixes to scan
            prefixes_to_scan = []
            if date_folders:
                # Scan today and yesterday to catch midnight boundary
                today = datetime.date.today()
                yesterday = today - datetime.timedelta(days=1)
                prefixes_to_scan.append(
                    _build_prefix_for_date(prefix, today, True)
                )
                prefixes_to_scan.append(
                    _build_prefix_for_date(prefix, yesterday, True)
                )
            else:
                # Flat folder — just use the base prefix
                prefixes_to_scan.append(prefix.rstrip("/") + "/")

            # List and process blobs
            new_points = []
            for scan_prefix in prefixes_to_scan:
                try:
                    blobs = list(gcs_bucket.list_blobs(prefix=scan_prefix))
                except Exception as e:
                    logger.warning(
                        f"[{track_key}] Failed to list blobs at "
                        f"'{scan_prefix}': {e}"
                    )
                    continue

                for blob in blobs:
                    blob_name = blob.name

                    # Skip directories
                    if blob_name.endswith("/"):
                        continue

                    # Skip already-seen blobs
                    if blob_name in _seen_blobs[track_key]:
                        continue

                    # Apply filename filter
                    basename = os.path.basename(blob_name)
                    if not fnmatch.fnmatch(basename.lower(), filename_pattern.lower()):
                        continue

                    # Make it public and get the URL
                    try:
                        blob.make_public()
                        public_url = blob.public_url
                    except Exception as e:
                        logger.warning(
                            f"[{track_key}] Could not make blob public "
                            f"'{blob_name}': {e}"
                        )
                        continue

                    # Get the timestamp
                    ts = _get_blob_timestamp(
                        blob, timestamp_source, timestamp_regex,
                        timestamp_format
                    )

                    # Create InfluxDB point
                    point = (
                        Point("sensor_image")
                        .tag("sensor_id", sensor_id)
                        .tag("view", view_name)
                        .field("image_url", public_url)
                        .field("filename", basename)
                        .time(ts)
                    )
                    new_points.append(point)
                    _seen_blobs[track_key].add(blob_name)

            # Write to InfluxDB
            if new_points:
                try:
                    write_api.write(
                        bucket=influx_bucket,
                        record=new_points
                    )
                    logger.info(
                        f"[{track_key}] Ingested {len(new_points)} new images"
                    )
                except Exception as e:
                    logger.error(
                        f"[{track_key}] Failed to write image points "
                        f"to InfluxDB: {e}"
                    )
                    # Remove from seen so they get retried next cycle
                    for pt in new_points:
                        _seen_blobs[track_key].discard(pt)


def start_image_poller_thread(
    sensor_configs: Dict[str, Dict[str, Any]],
    write_api: Any,
    bucket_name: str,
    influx_bucket: str,
    poll_interval: int = 120
) -> threading.Thread:
    """
    Start the image poller in a daemon thread.
    Returns the thread object.
    """
    thread = threading.Thread(
        target=poll_sensor_images,
        args=(sensor_configs, write_api, bucket_name, influx_bucket, poll_interval),
        name="image-poller",
        daemon=True
    )
    thread.start()
    logger.info("Image poller thread started.")
    return thread
