
import os
import time
import datetime
import re
import json
import logging
from typing import Dict, Any, List, Optional

import yaml

import firebase_admin
from firebase_admin import credentials, db
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Configuration
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL")
SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "serviceAccountKey.json")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# Sensor Config
SENSOR_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "sensor_config.yaml")

def load_sensor_config() -> Dict[str, Dict[str, Any]]:
    """
    Load sensor config from sensor_config.yaml.
    Returns a dict keyed by sensor ID with full config for each enabled sensor.
    """
    if not os.path.exists(SENSOR_CONFIG_PATH):
        logger.warning(f"Config not found at {SENSOR_CONFIG_PATH}, using fallback.")
        # Fallback: minimal config for legacy sensors
        return {
            "floodmonitor1": {"enabled": True},
            "floodmonitor2": {"enabled": True},
            "LoRaWAN":       {"enabled": True},
        }
    
    with open(SENSOR_CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f) or {}
    
    all_sensors = config.get("sensors", {})
    enabled = {sid: meta for sid, meta in all_sensors.items() if meta.get("enabled", False)}
    
    if not enabled:
        logger.warning("No enabled sensors in config! Check sensor_config.yaml.")
    else:
        logger.info(f"Loaded {len(enabled)} sensors from config: {list(enabled.keys())}")
    
    return enabled


# ── Field extraction helpers ─────────────────────────────────

_DISTANCE_FALLBACK_FIELDS = ["dist_cm", "distance_cm", "ultrasound", "ultrasound_cm",
                              "level", "level_cm", "distance", "water_level",
                              "distance_from_sensor"]
_BATTERY_FALLBACK_FIELDS  = ["bat_volt", "battery_voltage", "battery", "voltage"]
_SOLAR_FALLBACK_FIELDS    = ["solar_volt", "solar_voltage", "solar"]


def _get_field(data: Dict[str, Any], field_name: Optional[str], fallbacks: List[str]) -> Any:
    """Get a value from data using an explicit field name or a fallback chain."""
    if field_name:
        return data.get(field_name)
    for fb in fallbacks:
        val = data.get(fb)
        if val is not None:
            return val
    return None


def _parse_distance(raw_dist: Any, encoding: str) -> int:
    """
    Parse a raw distance value into an integer centimetre value.
    
    encoding:
      "string"  — extract digits from strings like "R0231"
      "numeric" — cast directly to int
      "auto"    — detect automatically
    """
    if raw_dist is None:
        return 0

    if encoding == "numeric":
        try:
            return int(float(raw_dist))
        except (ValueError, TypeError):
            return 0

    if encoding == "string":
        if isinstance(raw_dist, (int, float)):
            return int(raw_dist)
        match = re.search(r'\d+', str(raw_dist))
        return int(match.group()) if match else 0

    # encoding == "auto"
    if isinstance(raw_dist, (int, float)):
        return int(raw_dist)
    if isinstance(raw_dist, str):
        match = re.search(r'\d+', raw_dist)
        return int(match.group()) if match else 0
    return 0


def _parse_timestamp(raw_ts: Any, fmt: str) -> datetime.datetime:
    """
    Parse a raw timestamp into a timezone-aware datetime.
    
    fmt:
      "epoch_s"    — seconds since epoch
      "epoch_ms"   — milliseconds since epoch
      "epoch_auto" — auto-detect seconds vs milliseconds
      "iso"        — ISO-like string (e.g. "2026-02-19 13:55:25")
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    if raw_ts is None:
        return now

    if fmt == "iso":
        try:
            # Handle common ISO-like formats
            ts_str = str(raw_ts).strip()
            for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f",
                            "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    dt = datetime.datetime.strptime(ts_str, pattern)
                    return dt.replace(tzinfo=datetime.timezone.utc)
                except ValueError:
                    continue
            logger.warning(f"Could not parse ISO timestamp: {raw_ts}")
            return now
        except Exception:
            return now

    # Epoch-based formats
    try:
        ts = float(raw_ts)
    except (ValueError, TypeError):
        return now

    if fmt == "epoch_ms":
        ts = ts / 1000.0
    elif fmt == "epoch_auto":
        if ts > 1e11:
            ts = ts / 1000.0
    # else: epoch_s — use as-is

    try:
        return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
    except (OSError, OverflowError):
        return now


def clean_sensor_data(key: str, data: Dict[str, Any],
                      sensor_cfg: Optional[Dict[str, Any]] = None) -> Optional[Point]:
    """
    Normalizes sensor data and converts it to an InfluxDB Point.
    Uses per-sensor config from sensor_config.yaml when available,
    falls back to legacy heuristic chain otherwise.
    """
    try:
        # 1. Extract Sensor ID from path
        sensor_id = key.split('/')[1] if '/' in key else "unknown_sensor"

        # 2. Read field mappings from config (or use None for fallback)
        fields_cfg = (sensor_cfg or {}).get("fields", {})
        dist_field = fields_cfg.get("distance")
        bat_field  = fields_cfg.get("battery")
        sol_field  = fields_cfg.get("solar")
        ts_field   = fields_cfg.get("timestamp", "timestamp")

        dist_encoding = (sensor_cfg or {}).get("distance_encoding", "auto")
        ts_format     = (sensor_cfg or {}).get("timestamp_format", "epoch_auto")

        # 3. Extract values
        raw_dist = _get_field(data, dist_field, _DISTANCE_FALLBACK_FIELDS)
        raw_bat  = _get_field(data, bat_field,  _BATTERY_FALLBACK_FIELDS)
        raw_sol  = _get_field(data, sol_field,  _SOLAR_FALLBACK_FIELDS)
        raw_ts   = data.get(ts_field, time.time())

        # 4. Parse
        dist_cm = _parse_distance(raw_dist, dist_encoding)
        bat     = float(raw_bat) if raw_bat is not None else 0.0
        sol     = float(raw_sol) if raw_sol is not None else 0.0
        dt_obj  = _parse_timestamp(raw_ts, ts_format)

        # 5. Create InfluxDB Point
        point = Point("sensor_reading") \
            .tag("sensor_id", sensor_id) \
            .field("dist_cm", dist_cm) \
            .field("bat_volt", bat) \
            .field("solar_volt", sol) \
            .field("fb_key", str(key.split('/')[-1])) \
            .time(dt_obj)
        
        return point

    except Exception as e:
        logger.error(f"Error processing data for key {key}: {e}")
        return None

def main():
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
        org=org
    )
    write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=100))
    logger.info("Connected to InfluxDB.")

    # 2. Initialize Firebase
    if not os.path.exists(SERVICE_ACCOUNT_KEY):
        logger.error(f"Service Account Key not found at {SERVICE_ACCOUNT_KEY}")
        # For dev purposes, continuing might fail, but let's warn.
        return

    cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
    firebase_admin.initialize_app(cred, {
        'databaseURL': FIREBASE_DB_URL
    })
    logger.info(f"Connected to Firebase at {FIREBASE_DB_URL}")

    # 3. Load sensor config
    sensor_configs = load_sensor_config()
    SENSORS = list(sensor_configs.keys())

    # 4. Define Listener
    def listener(event):
        """
        Callback for Firebase events.
        """
        try:
            if event.data is None:
                return

            logger.info(f"Received event: {event.event_type} at {event.path}")
            
            # We only care about new data (put)
            if event.event_type == 'put':
                # CASE 1: Root Update (Initial Load or Massive Push)
                if event.path == "/":
                    if isinstance(event.data, dict):
                        points = []
                        logger.info(f"Processing ROOT update with {len(event.data)} sensors.")
                        for sensor_key, sensor_data in event.data.items():
                            cfg = sensor_configs.get(sensor_key)
                            if isinstance(sensor_data, dict):
                                 for push_id, record in sensor_data.items():
                                     if isinstance(record, dict):
                                         pt = clean_sensor_data(f"/{sensor_key}/{push_id}", record, cfg)
                                         if pt: points.append(pt)
                        
                        if points:
                            write_api.write(bucket=bucket, record=points)
                            logger.info(f"Backfilled {len(points)} records from ROOT update.")
                
                # CASE 2: Specific Record Update (e.g., /floodmonitor1/-Oa3...)
                else:
                    parts = event.path.strip('/').split('/')
                    
                    if len(parts) == 1:
                        sensor_id = parts[0]
                        cfg = sensor_configs.get(sensor_id)
                        if isinstance(event.data, dict):
                            points = []
                            for push_id, record in event.data.items():
                                if isinstance(record, dict):
                                    pt = clean_sensor_data(f"/{sensor_id}/{push_id}", record, cfg)
                                    if pt: points.append(pt)
                            if points:
                                write_api.write(bucket=bucket, record=points)
                                logger.info(f"Wrote {len(points)} records for sensor {sensor_id}")

                    elif len(parts) == 2:
                        sensor_id = parts[0]
                        cfg = sensor_configs.get(sensor_id)
                        pt = clean_sensor_data(event.path, event.data, cfg)
                        if pt:
                            write_api.write(bucket=bucket, record=pt)
                            logger.info(f"Wrote 1 record: {event.path}")
                            
        except Exception as e:
            logger.error(f"Listener Error: {e}")

    # 5. Start Polling Loop
    STATE_FILE = "src/bridge_state.json"
    cursors = {} # {sensor_id: last_seen_key}

    # Helper to load/save state
    def load_state_from_file():
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load state file: {e}")
        return {}

    def save_state(cursors_data):
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(cursors_data, f)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

    def fetch_last_key_from_influx(sensor_id):
        """
        Queries InfluxDB to find the last 'fb_key' written for this sensor.
        """
        query_api = influx_client.query_api()
        q = f"""
        from(bucket: "{bucket}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
          |> filter(fn: (r) => r["sensor_id"] == "{sensor_id}")
          |> filter(fn: (r) => r["_field"] == "fb_key")
          |> last()
        """
        try:
            result = query_api.query(q)
            if result:
                for table in result:
                    for record in table.records:
                        return record.get_value()
        except Exception as e:
            logger.warning(f"[{sensor_id}] Could not fetch last key from InfluxDB: {e}")
        return None

    # Initialize cursors
    saved_state = load_state_from_file()
    logger.info("Initializing cursors...")
    
    for sensor in SENSORS:
        # Priority 1: Local State File
        if sensor in saved_state:
            cursors[sensor] = saved_state[sensor]
            logger.info(f"[{sensor}] Resuming from LOCAL FILE cursor: {cursors[sensor]}")
        
        else:
            # Priority 2: InfluxDB (Recovery)
            influx_key = fetch_last_key_from_influx(sensor)
            if influx_key:
                cursors[sensor] = influx_key
                logger.info(f"[{sensor}] Resuming from INFLUXDB cursor: {cursors[sensor]}")
            
            # Priority 3: Fresh Start (Latest)
            else:
                try:
                    snapshot = db.reference(f"/{sensor}").order_by_key().limit_to_last(1).get()
                    if snapshot and isinstance(snapshot, dict):
                        last_key = list(snapshot.keys())[-1]
                        cursors[sensor] = last_key
                        logger.info(f"[{sensor}] No history found. Starting FRESH at: {last_key}")
                    else:
                        cursors[sensor] = None 
                        logger.info(f"[{sensor}] No data found on Firebase. Starting fresh.")
                except Exception as e:
                    logger.error(f"Failed to fetch initial cursor for {sensor}: {e}")
                    cursors[sensor] = None

    # Initial save
    save_state(cursors)
    logger.info("Starting Polling Loop...")

    try:
        while True:
            for sensor in SENSORS:
                try:
                    ref = db.reference(f"/{sensor}")
                    query = ref.order_by_key()
                    
                    last_key = cursors.get(sensor)
                    if last_key:
                        # Fetch records starting AFTER the last known key
                        # Firebase only has start_at, so we fetch starting at last_key
                        # and filter it out locally.
                        query = query.start_at(last_key).limit_to_first(100)
                    else:
                        # If no history, just get current/new
                        query = query.limit_to_last(100)

                    snapshot = query.get()

                    if snapshot and isinstance(snapshot, dict):
                        # If snapshot returns the exact same single key as last_key, nothing new
                        if len(snapshot) == 1 and last_key in snapshot:
                            continue
                            
                        # Process records
                        points = []
                        new_last_key = last_key
                        
                        # Sort keys to ensure order (get() might return OrderedDict or dict)
                        sorted_keys = sorted(snapshot.keys())
                        
                        for key in sorted_keys:
                            if key == last_key:
                                continue # Skip the cursor itself
                            
                            record = snapshot[key]
                            if isinstance(record, dict):
                                cfg = sensor_configs.get(sensor)
                                pt = clean_sensor_data(f"/{sensor}/{key}", record, cfg)
                                if pt: points.append(pt)
                            
                            new_last_key = key # Advance cursor
                        
                        if points:
                            write_api.write(bucket=bucket, record=points)
                            logger.info(f"[{sensor}] Processed {len(points)} new records.")
                            cursors[sensor] = new_last_key
                            save_state(cursors)
                            
                except Exception as e:
                    logger.error(f"Error polling {sensor}: {e}")
            
            # Sleep to prevent high CPU usage
            time.sleep(1.0)

    except KeyboardInterrupt:
        logger.info("Stopping bridge service...")
        influx_client.close()

if __name__ == "__main__":
    main()
