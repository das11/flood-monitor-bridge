import { InfluxDB } from '@influxdata/influxdb-client-browser';

const url = import.meta.env.VITE_INFLUX_URL;
const token = import.meta.env.VITE_INFLUX_TOKEN;
const org = import.meta.env.VITE_INFLUX_ORG;
const bucket = import.meta.env.VITE_INFLUX_BUCKET;

const client = new InfluxDB({ url, token });
const queryApi = client.getQueryApi(org);

export const fetchLatestReading = async (sensorId) => {
    if (!sensorId) return null;

    const fluxQuery = `
  from(bucket: "${bucket}")
    |> range(start: -24h)
    |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
    |> filter(fn: (r) => r["sensor_id"] == "${sensorId}")
    |> last()
  `;

    try {
        const rows = [];
        await new Promise((resolve, reject) => {
            queryApi.queryRows(fluxQuery, {
                next(row, tableMeta) {
                    const o = tableMeta.toObject(row);
                    rows.push(o);
                },
                error(error) {
                    console.error('InfluxDB Query Error:', error);
                    reject(error);
                },
                complete() {
                    resolve();
                },
            });
        });

        if (rows.length === 0) return null;

        // Process rows into a single object
        // Rows come in as multiple entries for different fields (_field: dist_cm, _field: bat_volt, etc.)
        const result = {
            level: 0,
            battery: 0,
            solar: 0,
            lastUpdated: null,
        };

        let latestTime = null;

        rows.forEach((row) => {
            if (row._field === 'dist_cm') result.level = row._value;
            if (row._field === 'bat_volt') result.battery = row._value;
            if (row._field === 'solar_volt') result.solar = row._value;

            const time = new Date(row._time);
            if (!latestTime || time > latestTime) {
                latestTime = time;
            }
        });

        if (latestTime) {
            // Format time relative to now (e.g., "5 mins ago")
            const diffMs = new Date() - latestTime;
            const diffMins = Math.floor(diffMs / 60000);

            if (diffMins < 1) result.lastUpdated = "Just now";
            else if (diffMins < 60) result.lastUpdated = `${diffMins} mins ago`;
            else {
                const diffHours = Math.floor(diffMins / 60);
                result.lastUpdated = `${diffHours} hours ago`;
            }
        }

        return result;

    } catch (error) {
        console.error(`Failed to fetch data for ${sensorId}:`, error);
        return null;
    }
};
// ... existing code ...

const getDurationQuery = (duration) => {
    switch (duration) {
        case '1h': return '-1h';
        case '6h': return '-6h';
        case '24h': return '-24h';
        case '7d': return '-7d';
        case '30d': return '-30d';
        default: return '-24h';
    }
};

export const fetchSensorHistory = async (sensorId, duration = '24h') => {
    if (!sensorId) return null;

    const rangeStart = getDurationQuery(duration);
    // Aggregate data to avoid transferring too many points for long durations
    let aggregateWindow = '1m';
    if (duration === '6h') aggregateWindow = '5m';
    if (duration === '24h') aggregateWindow = '15m';
    if (duration === '7d') aggregateWindow = '1h';
    if (duration === '30d') aggregateWindow = '4h';

    const fluxQuery = `
  from(bucket: "${bucket}")
    |> range(start: ${rangeStart})
    |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
    |> filter(fn: (r) => r["sensor_id"] == "${sensorId}")
    |> filter(fn: (r) => r["_field"] == "dist_cm")
    |> aggregateWindow(every: ${aggregateWindow}, fn: mean, createEmpty: false)
    |> yield(name: "mean")
  `;

    try {
        const history = [];
        await new Promise((resolve, reject) => {
            queryApi.queryRows(fluxQuery, {
                next(row, tableMeta) {
                    const o = tableMeta.toObject(row);
                    history.push({
                        time: new Date(o._time).getTime(), // Use timestamp for Recharts
                        level: Math.round(o._value * 10) / 10 // Round to 1 decimal
                    });
                },
                error(error) {
                    console.error('InfluxDB History Query Error:', error);
                    reject(error);
                },
                complete() {
                    resolve();
                },
            });
        });

        return history.length > 0 ? history : null;

    } catch (error) {
        console.error(`Failed to fetch history for ${sensorId}:`, error);
        return null;
    }
};

// ── Image timeline queries ──────────────────────────────────

export const fetchSensorImages = async (sensorId, view = 'front', duration = '24h') => {
    if (!sensorId) return null;

    const rangeStart = getDurationQuery(duration);
    // At 30D a sensor can produce 10k+ images, too many to render as thumbnails.
    // Evenly downsample to a manageable count instead of returning everything.
    const downsample = duration === '30d' ? '\n    |> sample(n: 60, pos: 0, column: "_time")' : '';

    const fluxQuery = `
  from(bucket: "${bucket}")
    |> range(start: ${rangeStart})
    |> filter(fn: (r) => r["_measurement"] == "sensor_image")
    |> filter(fn: (r) => r["sensor_id"] == "${sensorId}")
    |> filter(fn: (r) => r["view"] == "${view}")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> sort(columns: ["_time"])${downsample}
  `;

    try {
        const images = [];
        await new Promise((resolve, reject) => {
            queryApi.queryRows(fluxQuery, {
                next(row, tableMeta) {
                    const o = tableMeta.toObject(row);
                    images.push({
                        time: new Date(o._time).getTime(),
                        imageUrl: o.image_url,
                        filename: o.filename,
                        view: o.view || view,
                    });
                },
                error(error) {
                    console.error('InfluxDB Image Query Error:', error);
                    reject(error);
                },
                complete() {
                    resolve();
                },
            });
        });

        return images.length > 0 ? images : null;

    } catch (error) {
        console.error(`Failed to fetch images for ${sensorId}/${view}:`, error);
        return null;
    }
};
