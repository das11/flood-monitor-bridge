
# Flood Monitor Bridge 🌉

The **Flood Monitor Bridge** is a robust, stateless service that synchronizes real-time sensor data from **Firebase Realtime Database** to **InfluxDB**. It is the core data ingestion engine for the RT Sensing Dashboard.

---

## 🏗️ Architecture & Features

This service is designed to be **fault-tolerant** and **self-healing**. It handles intermittent connectivity, service restarts, and high-throughput data bursts without data loss.

### 1. 🔄 Cursor-Based Polling (No More Listeners)
Instead of using fragile WebSocket listeners (which can hang on large datasets), the bridge uses a **Smart Polling Loop**:
- **Frequency**: Every `1.0s`.
- **Logic**: Fetches only records *newer* than the last successfully processed key.
- **Batching**: Automatically handles bursts of up to **100 records per poll**.

### 2. 🛡️ Double-Layer State Recovery
The bridge "remembers" where it left off using a two-tier persistence strategy:
- **Tier 1 (Local Cache)**: Uses `src/bridge_state.json` to instantly resume from the last local checkpoint.
- **Tier 2 (InfluxDB Backup)**: If the container is destroyed (local cache lost), it queries **InfluxDB** for the last synced `fb_key`.
- **Tier 3 (Fresh Start)**: If no history exists, it starts processing new data from "Now".

**Result**: You can delete the container, restart it, and it will **auto-resume** exactly where it stopped.

### 3. 🧹 Universal Data Normalization
Sensors often send data in messy, inconsistent formats. The `clean_sensor_data` function acts as a universal adapter:
- **Field Aliases**: Automatically maps `dist_cm`, `ultrasound`, `distance`, `level` -> `dist_cm`.
- **Type Coercion**: Handles string values (e.g., `"R0452"`, `"12.5V"`) and raw numbers (`512`) interchangeably.
- **Timestamp correction**: Auto-detects **Seconds** vs **Milliseconds** and converts everything to UTC.

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- `serviceAccountKey.json` (Firebase Credentials)
- `.env` file (InfluxDB Credentials)

### 1. Run the Bridge
```bash
docker compose up -d --build
```

### 2. View Logs
```bash
docker compose logs -f bridge
```
*Look for: `[floodmonitor1] Resuming from INFLUXDB cursor...`*

---

## 🛠️ Development & Simulation

We include powerful tools to test the bridge without waiting for real rain.

### 🌊 Simulate Data (`src/simulate_data.py`)
Generates realistic sensor patterns (Sine Waves) and pushes them to Firebase.

**Instant Batch (Fast Mode):**
Generates 4 hours of data in <1 second.
```bash
docker compose exec bridge python src/simulate_data.py --mode run --sensor floodmonitor1 --count 120 --interval 120 --delay 0.1 --past
```
*Flags:*
- `--past`: Backfills data ending "now".
- `--delay 0.1`: Triggers **Batch Mode** (sends 120 records in 1 request).

**Real-Time Simulation:**
Sends 1 record every second.
```bash
docker compose exec bridge python src/simulate_data.py --mode run --sensor floodmonitor1 --count 100 --interval 5 --delay 1.0
```

### 🔍 Verify Data (`src/verify_data.py`)
Checks InfluxDB to confirm data arrival and integrity.
```bash
docker compose exec bridge python src/verify_data.py
```
*Output includes:*
- Record Counts per sensor.
- Min/Max values.
- **Last Synced Key** (proof of state recovery).

---

## 📊 Useful Queries (InfluxDB)

### View Last Synced Firebase Key
Use this Flux query to debug state recovery issues:

```flux
from(bucket: "flood_monitoring")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
  |> filter(fn: (r) => r["_field"] == "fb_key")
  |> last()
  |> group(columns: ["sensor_id"])
  |> yield(name: "last_synced_keys")
```

### Check Recent Data Arrival
```flux
from(bucket: "flood_monitoring")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
  |> filter(fn: (r) => r["_field"] == "dist_cm")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
  |> yield(name: "recent_trend")
```

## InfluxDB Point Schema
```
Bucket: flood_monitoring
│
└── _measurement: "sensor_reading"     ← the "table"
    │
    ├── Tags (indexed filters):
    │   └── sensor_id                  ← filter by sensor
    │
    ├── Fields (the data):
    │   ├── dist_cm
    │   ├── bat_volt
    │   ├── solar_volt
    │   └── fb_key
    │
    └── _time                          ← every point has a timestamp

```


## Useful Queries

### View Last Synced Firebase Key (State Recovery Check)

Run this in **InfluxDB Data Explorer** to see the last `fb_key` recorded for each sensor:

```flux
from(bucket: "flood_monitoring")
  |> range(start: -30d)
  |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
  |> filter(fn: (r) => r["_field"] == "fb_key")
  |> last()
  |> group(columns: ["sensor_id"])
  |> yield(name: "last_synced_keys")
```


