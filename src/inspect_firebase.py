#!/usr/bin/env python3
"""
Firebase RTDB Inspector
=======================
Discovers ALL root-level keys in Firebase, samples child records,
analyses data structure, and compares against sensor_config.yaml.

Usage:
    uv run python src/inspect_firebase.py
    uv run python src/inspect_firebase.py --samples 5
"""

import os
import re
import sys
import json
import argparse
import datetime
from collections import Counter
from typing import Any, Dict, List, Optional, Set

import yaml
import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv

# ── Styling helpers ──────────────────────────────────────────

BOLD   = "\033[1m"
DIM    = "\033[2m"
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
MAGENTA = "\033[95m"
RESET  = "\033[0m"
CHECK  = f"{GREEN}✓{RESET}"
WARN   = f"{YELLOW}⚠{RESET}"
CROSS  = f"{RED}✗{RESET}"
INFO   = f"{CYAN}ℹ{RESET}"

SEPARATOR = f"{DIM}{'─' * 70}{RESET}"
SECTION_SEP = f"\n{BOLD}{'═' * 70}{RESET}"


def header(title: str):
    print(f"{SECTION_SEP}")
    print(f"  {BOLD}{title}{RESET}")
    print(f"{'═' * 70}\n")


def sub_header(title: str):
    print(f"\n  {BOLD}{CYAN}{title}{RESET}")
    print(f"  {DIM}{'─' * 60}{RESET}")

# ── Config loader ────────────────────────────────────────────

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "sensor_config.yaml")


def load_config() -> Dict[str, Any]:
    """Load sensor_config.yaml and return the sensors dict."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f) or {}
    return data.get("sensors", {})


# ── Analyzers ────────────────────────────────────────────────

KNOWN_FLOODMONITOR_FIELDS = {"ultrasound", "ultrasound_cm", "bat_volt", "solar_volt", "timestamp",
                              "load_current", "load_power", "load_volt",
                              "load_current_ma", "load_power_mW",
                              "solar_current", "solar_power",
                              "solar_current_ma", "solar_power_mW",
                              "simulated"}
KNOWN_LORAWAN_FIELDS = {"distance_cm", "dist_cm", "packet_number", "timestamp",
                         "device_id", "simulated"}
KNOWN_NADI_FIELDS = {"water_level", "level_cm", "distance_from_sensor",
                      "image_url", "photo_url", "status", "timestamp"}


def classify_timestamp(value: Any) -> str:
    """Detect timestamp format."""
    try:
        ts = float(value)
        if ts > 1e15:
            return "microseconds"
        elif ts > 1e12:
            return "milliseconds"
        elif ts > 1e9:
            return "seconds"
        else:
            return f"unknown ({ts})"
    except (ValueError, TypeError):
        # Check for ISO-like datetime strings
        if isinstance(value, str):
            ts_str = value.strip()
            for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f",
                            "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    datetime.datetime.strptime(ts_str, pattern)
                    return "iso-datetime"
                except ValueError:
                    continue
        return f"unparseable ({value!r})"


def classify_distance(value: Any) -> str:
    """Detect distance encoding."""
    if isinstance(value, (int, float)):
        return "numeric"
    elif isinstance(value, str):
        if re.match(r'^[A-Z]*\d+$', value):
            return f"encoded-string (e.g. {value!r})"
        return f"raw-string ({value!r})"
    return f"unknown-type ({type(value).__name__})"


def detect_format(fields: Set[str]) -> str:
    """Guess the sensor format based on field names."""
    if "ultrasound" in fields or "ultrasound_cm" in fields:
        return "floodmonitor"
    if "distance_cm" in fields or "dist_cm" in fields:
        if "device_id" in fields or "packet_number" in fields:
            return "lorawan"
        return "lorawan-like"
    if "level_cm" in fields or "water_level" in fields:
        return "nadi-mitra"
    return "unknown"


def analyse_records(records: List[Dict[str, Any]], sensor_key: str) -> Dict[str, Any]:
    """
    Analyse a list of sampled records for a single sensor.
    Returns a structured analysis dict.
    """
    analysis: Dict[str, Any] = {
        "record_count": len(records),
        "field_schemas": [],
        "alerts": [],
        "detected_format": "unknown",
        "timestamp_formats": [],
        "distance_encodings": [],
    }

    if not records:
        analysis["alerts"].append(("error", "No child records found"))
        return analysis

    # Track field sets per record to detect inconsistencies
    field_sets: List[Set[str]] = []
    ts_formats = Counter()
    dist_encodings = Counter()

    for record in records:
        if not isinstance(record, dict):
            analysis["alerts"].append(("error", f"Non-dict child found: {type(record).__name__} → {str(record)[:80]}"))
            continue

        fields = set(record.keys())
        field_sets.append(fields)

        # Timestamp analysis
        ts_val = record.get("timestamp")
        if ts_val is not None:
            ts_formats[classify_timestamp(ts_val)] += 1
        else:
            ts_formats["missing"] += 1

        # Distance analysis — search all known distance field names
        for dist_key in ("dist_cm", "distance_cm", "ultrasound", "ultrasound_cm",
                         "level", "level_cm", "distance", "water_level",
                         "distance_from_sensor"):
            if dist_key in record:
                dist_encodings[classify_distance(record[dist_key])] += 1
                break

    # Detect mixed schemas (field sets that differ between records)
    if len(field_sets) > 1:
        unique_schemas = [frozenset(fs) for fs in field_sets]
        unique_count = len(set(unique_schemas))
        if unique_count > 1:
            analysis["alerts"].append((
                "warning",
                f"Mixed field schemas detected! {unique_count} different structures found across {len(records)} records"
            ))
            # Show the different schemas
            for i, schema in enumerate(sorted(set(unique_schemas), key=lambda s: -len(s))):
                count = unique_schemas.count(schema)
                analysis["field_schemas"].append({
                    "variant": i + 1,
                    "count": count,
                    "fields": sorted(schema),
                })
        else:
            analysis["field_schemas"].append({
                "variant": 1,
                "count": len(records),
                "fields": sorted(field_sets[0]),
            })
    elif field_sets:
        analysis["field_schemas"].append({
            "variant": 1,
            "count": len(records),
            "fields": sorted(field_sets[0]),
        })

    # Detect format
    all_fields = set()
    for fs in field_sets:
        all_fields.update(fs)
    analysis["detected_format"] = detect_format(all_fields)

    # Store distributions
    analysis["timestamp_formats"] = dict(ts_formats)
    analysis["distance_encodings"] = dict(dist_encodings)

    # Check for unknown fields
    known = KNOWN_FLOODMONITOR_FIELDS | KNOWN_LORAWAN_FIELDS | KNOWN_NADI_FIELDS
    unknown = all_fields - known
    if unknown:
        analysis["alerts"].append((
            "info",
            f"Unknown fields: {', '.join(sorted(unknown))} — bridge may ignore these"
        ))

    return analysis


# ── Display ──────────────────────────────────────────────────

def display_sensor_report(sensor_key: str, record_count: int, analysis: Dict, config_entry: Optional[Dict]):
    """Pretty-print the analysis for one sensor."""

    # Header
    config_desc = config_entry.get("description", "") if config_entry else ""
    status_icon = CHECK if config_entry and config_entry.get("enabled") else (WARN if config_entry else CROSS)
    config_status = "Configured & Enabled" if (config_entry and config_entry.get("enabled")) \
        else ("Configured but DISABLED" if config_entry else "NOT in config")

    print(f"\n  {BOLD}{MAGENTA}┌─ {sensor_key}{RESET}")
    print(f"  {MAGENTA}│{RESET}  {DIM}Firebase records:{RESET} ~{record_count}   {DIM}|{RESET}   {status_icon} {config_status}")
    if config_desc:
        print(f"  {MAGENTA}│{RESET}  {DIM}Description:{RESET} {config_desc}")

    # Detected format
    detected = analysis["detected_format"]
    print(f"  {MAGENTA}│{RESET}")
    print(f"  {MAGENTA}│{RESET}  {BOLD}Detected type:{RESET}  {CYAN}{detected}{RESET}")

    # Field schemas
    print(f"  {MAGENTA}│{RESET}")
    for schema in analysis["field_schemas"]:
        variant_label = f"Schema v{schema['variant']}" if len(analysis["field_schemas"]) > 1 else "Fields"
        count_label = f" ({schema['count']}/{analysis['record_count']} records)" if len(analysis["field_schemas"]) > 1 else ""
        print(f"  {MAGENTA}│{RESET}  {BOLD}{variant_label}{RESET}{count_label}:")
        # Show fields in a neat grid
        fields = schema["fields"]
        line = "  "
        for f in fields:
            addition = f"{DIM}•{RESET} {f}   "
            if len(line) + len(f) + 5 > 65:
                print(f"  {MAGENTA}│{RESET}    {line}")
                line = "  "
            line += addition
        if line.strip():
            print(f"  {MAGENTA}│{RESET}    {line}")

    # Timestamp & Distance
    if analysis["timestamp_formats"]:
        print(f"  {MAGENTA}│{RESET}")
        ts_str = ", ".join(f"{fmt}: {cnt}" for fmt, cnt in analysis["timestamp_formats"].items())
        print(f"  {MAGENTA}│{RESET}  {BOLD}Timestamps:{RESET}  {ts_str}")

    if analysis["distance_encodings"]:
        dist_str = ", ".join(f"{enc}: {cnt}" for enc, cnt in analysis["distance_encodings"].items())
        print(f"  {MAGENTA}│{RESET}  {BOLD}Distance:{RESET}    {dist_str}")

    # Alerts
    if analysis["alerts"]:
        print(f"  {MAGENTA}│{RESET}")
        for level, msg in analysis["alerts"]:
            if level == "error":
                icon = CROSS
            elif level == "warning":
                icon = WARN
            else:
                icon = INFO
            print(f"  {MAGENTA}│{RESET}  {icon} {msg}")

    print(f"  {MAGENTA}└{'─' * 60}{RESET}")


def display_sample_record(record: Dict, key: str):
    """Pretty-print a single sample record."""
    print(f"      {DIM}Key:{RESET} {key}")
    for field, value in sorted(record.items()):
        val_type = type(value).__name__
        val_str = str(value)
        if len(val_str) > 60:
            val_str = val_str[:57] + "..."
        print(f"        {field}: {CYAN}{val_str}{RESET}  {DIM}({val_type}){RESET}")
    print()


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Inspect Firebase RTDB sensor structure")
    parser.add_argument("--samples", type=int, default=3,
                        help="Number of recent records to sample per sensor (default: 3)")
    parser.add_argument("--show-samples", action="store_true",
                        help="Print raw sample records for each sensor")
    args = parser.parse_args()

    # Load env
    load_dotenv()
    firebase_db_url = os.getenv("FIREBASE_DB_URL")
    service_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "serviceAccountKey.json")

    if not firebase_db_url:
        print(f"  {CROSS} FIREBASE_DB_URL not set in .env")
        sys.exit(1)

    if not os.path.exists(service_key):
        print(f"  {CROSS} Service account key not found: {service_key}")
        sys.exit(1)

    # Init Firebase
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(service_key)
        firebase_admin.initialize_app(cred, {"databaseURL": firebase_db_url})

    # Load config
    config = load_config()

    # ── 1. Discover root keys ────────────────────────────────

    header("Firebase RTDB — Sensor Discovery & Analysis")

    print(f"  {INFO} Database: {CYAN}{firebase_db_url}{RESET}")
    print(f"  {INFO} Config:   {CYAN}{os.path.abspath(CONFIG_PATH)}{RESET}")
    print(f"  {INFO} Time:     {DIM}{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print()

    print(f"  {BOLD}Discovering root-level keys...{RESET}")
    root_ref = db.reference("/")
    # Use shallow=True to get just the keys without downloading all data
    # Firebase Admin SDK doesn't support shallow directly, so we fetch root
    # and just use the keys. For large DBs, this could be slow.
    # We'll use limit + order to keep it reasonable.

    # Get root keys via REST-like approach: fetch with shallow
    root_snapshot = root_ref.get(shallow=True)

    if not root_snapshot:
        print(f"  {CROSS} No data found at root of RTDB!")
        sys.exit(1)

    firebase_keys = sorted(root_snapshot.keys()) if isinstance(root_snapshot, dict) else []
    print(f"  {CHECK} Found {BOLD}{len(firebase_keys)}{RESET} root-level keys: {', '.join(firebase_keys)}")

    # ── 2. Config comparison ─────────────────────────────────

    sub_header("Config vs Firebase Comparison")

    config_keys = set(config.keys())
    fb_keys_set = set(firebase_keys)

    on_fb_not_config = fb_keys_set - config_keys
    in_config_not_fb = config_keys - fb_keys_set
    matched = fb_keys_set & config_keys

    for key in sorted(matched):
        enabled = config[key].get("enabled", False)
        icon = CHECK if enabled else f"{DIM}○{RESET}"
        label = "enabled" if enabled else f"{DIM}disabled{RESET}"
        print(f"    {icon} {key}  →  {label}")

    if on_fb_not_config:
        print()
        for key in sorted(on_fb_not_config):
            print(f"    {WARN} {YELLOW}{key}{RESET}  →  on Firebase but {BOLD}NOT in sensor_config.yaml{RESET}")
        print(f"\n    {DIM}Tip: Add these to sensor_config.yaml if they are real sensors{RESET}")

    if in_config_not_fb:
        print()
        for key in sorted(in_config_not_fb):
            print(f"    {CROSS} {RED}{key}{RESET}  →  in config but {BOLD}NOT found on Firebase{RESET}")

    if not on_fb_not_config and not in_config_not_fb:
        print(f"\n    {CHECK} {GREEN}Config and Firebase are in sync!{RESET}")

    # ── 3. Per-sensor analysis ───────────────────────────────

    sub_header(f"Per-Sensor Structure Analysis  ({args.samples} samples each)")

    all_alerts = []

    for sensor_key in firebase_keys:
        sensor_ref = db.reference(f"/{sensor_key}")

        # Get total record count (approximate — fetch keys only)
        try:
            count_snapshot = sensor_ref.get(shallow=True)
            record_count = len(count_snapshot) if isinstance(count_snapshot, dict) else 0
        except Exception:
            record_count = 0

        # Sample recent records
        try:
            sample_query = sensor_ref.order_by_key().limit_to_last(args.samples)
            sample_snapshot = sample_query.get()
        except Exception as e:
            print(f"  {CROSS} Failed to sample {sensor_key}: {e}")
            continue

        records = []
        sample_keys = []
        if isinstance(sample_snapshot, dict):
            for k in sorted(sample_snapshot.keys()):
                val = sample_snapshot[k]
                if isinstance(val, dict):
                    records.append(val)
                    sample_keys.append(k)

        # Analyse
        config_entry = config.get(sensor_key)
        analysis = analyse_records(records, sensor_key)

        # Display report
        display_sensor_report(sensor_key, record_count, analysis, config_entry)

        # Show raw samples if requested
        if args.show_samples and records:
            print(f"\n    {DIM}Sample records:{RESET}")
            for i, (rec, key) in enumerate(zip(records, sample_keys)):
                display_sample_record(rec, key)

        # Collect alerts for summary
        for level, msg in analysis["alerts"]:
            all_alerts.append((sensor_key, level, msg))

    # ── 4. Summary ───────────────────────────────────────────

    header("Summary")

    total_sensors = len(firebase_keys)
    configured = len(matched)
    unconfigured = len(on_fb_not_config)
    missing = len(in_config_not_fb)

    print(f"  Sensors on Firebase:     {BOLD}{total_sensors}{RESET}")
    print(f"  Configured & matched:    {GREEN}{configured}{RESET}")
    if unconfigured:
        print(f"  Unconfigured (new?):     {YELLOW}{unconfigured}{RESET}  ← action needed")
    if missing:
        print(f"  Config-only (stale?):    {RED}{missing}{RESET}  ← check config")

    if all_alerts:
        print(f"\n  {BOLD}Alerts:{RESET}")
        for sensor, level, msg in all_alerts:
            if level == "error":
                icon = CROSS
            elif level == "warning":
                icon = WARN
            else:
                icon = INFO
            print(f"    {icon} [{sensor}] {msg}")
    else:
        print(f"\n  {CHECK} {GREEN}No alerts — all sensors look healthy!{RESET}")

    # Suggestion block
    if on_fb_not_config:
        print(f"\n  {BOLD}Suggested additions to sensor_config.yaml:{RESET}\n")
        for key in sorted(on_fb_not_config):
            # Try to guess format from analysis
            sensor_ref = db.reference(f"/{key}")
            first_val = None
            try:
                sample = sensor_ref.order_by_key().limit_to_last(1).get()
                if isinstance(sample, dict):
                    first_val = list(sample.values())[0]
                    if isinstance(first_val, dict):
                        fmt = detect_format(set(first_val.keys()))
                    else:
                        fmt = "unknown"
                else:
                    fmt = "unknown"
            except Exception:
                fmt = "unknown"

            print(f"  {DIM}# Add to sensor_config.yaml under 'sensors:':{RESET}")
            print(f"  {CYAN}{key}:{RESET}")
            print(f"    {CYAN}enabled: false{RESET}")
            print(f"    {CYAN}description: \"TODO: describe this sensor\"{RESET}")

            # Suggest field mappings based on detected fields
            if first_val is not None and isinstance(first_val, dict):
                fv_keys = set(first_val.keys())
                # Guess distance field
                dist_field = None
                for df in ("dist_cm", "distance_cm", "ultrasound", "ultrasound_cm",
                           "level_cm", "water_level", "distance_from_sensor", "level", "distance"):
                    if df in fv_keys:
                        dist_field = df
                        break
                # Guess battery field
                bat_field = None
                for bf in ("bat_volt", "battery_voltage", "battery", "voltage"):
                    if bf in fv_keys:
                        bat_field = bf
                        break
                # Guess solar field
                sol_field = None
                for sf in ("solar_volt", "solar_voltage", "solar"):
                    if sf in fv_keys:
                        sol_field = sf
                        break
                # Guess timestamp format
                ts_val = first_val.get("timestamp")
                ts_fmt = "epoch_auto"
                if ts_val is not None:
                    try:
                        float(ts_val)
                        ts_class = classify_timestamp(ts_val)
                        if "seconds" in ts_class:
                            ts_fmt = "epoch_s"
                        elif "milliseconds" in ts_class:
                            ts_fmt = "epoch_ms"
                    except (ValueError, TypeError):
                        ts_fmt = "iso"
                # Guess distance encoding
                dist_enc = "auto"
                if dist_field and dist_field in first_val:
                    dv = first_val[dist_field]
                    if isinstance(dv, (int, float)):
                        dist_enc = "numeric"
                    elif isinstance(dv, str) and re.match(r'^[A-Z]*\d+$', dv):
                        dist_enc = "string"

                print(f"    {CYAN}fields:{RESET}")
                if dist_field:
                    print(f"      {CYAN}distance: \"{dist_field}\"{RESET}")
                if bat_field:
                    print(f"      {CYAN}battery: \"{bat_field}\"{RESET}")
                if sol_field:
                    print(f"      {CYAN}solar: \"{sol_field}\"{RESET}")
                print(f"      {CYAN}timestamp: \"timestamp\"{RESET}")
                print(f"    {CYAN}distance_encoding: \"{dist_enc}\"{RESET}")
                print(f"    {CYAN}timestamp_format: \"{ts_fmt}\"{RESET}")
            print()

    print(f"\n{DIM}Done.{RESET}\n")


if __name__ == "__main__":
    main()
