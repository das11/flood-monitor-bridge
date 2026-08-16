import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { fetchSensorHistory } from '../services/influxService';
import PropTypes from 'prop-types';

const RANGES = [
    { label: '1H', value: '1h' },
    { label: '6H', value: '6h' },
    { label: '24H', value: '24h' },
    { label: '7D', value: '7d' },
    { label: '30D', value: '30d' },
];

const WaterLevelGraph = ({ sensorId, sensorName }) => {
    const [data, setData] = useState([]);
    const [timeRange, setTimeRange] = useState('24h');
    const [loading, setLoading] = useState(true);
    const [isOffline, setIsOffline] = useState(false);

    useEffect(() => {
        let isMounted = true;

        const loadData = async () => {
            setLoading(true);
            if (!sensorId) return;

            const history = await fetchSensorHistory(sensorId, timeRange);

            if (isMounted) {
                if (history && history.length > 0) {
                    setData(history);
                    setIsOffline(false);
                } else {
                    // Generate realistic offline mock data (sine wave)
                    const mockData = generateMockData(timeRange);
                    setData(mockData);
                    setIsOffline(true);
                }
                setLoading(false);
            }
        };

        loadData();

        // Refresh every minute if live
        const interval = setInterval(loadData, 60000);
        return () => {
            isMounted = false;
            clearInterval(interval);
        };
    }, [sensorId, timeRange]);

    // Helper to generate smooth sine wave for offline mode
    const generateMockData = (range) => {
        const points = 50;
        const now = Date.now();
        const result = [];
        let durationMs = 24 * 60 * 60 * 1000;
        if (range === '1h') durationMs = 60 * 60 * 1000;
        if (range === '6h') durationMs = 6 * 60 * 60 * 1000;
        if (range === '7d') durationMs = 7 * 24 * 60 * 60 * 1000;
        if (range === '30d') durationMs = 30 * 24 * 60 * 60 * 1000;

        const step = durationMs / points;

        for (let i = points; i >= 0; i--) {
            const time = now - (i * step);
            // Sine wave oscillating between 300 and 380
            const level = 340 + Math.sin(i * 0.5) * 40 + (Math.random() * 5);
            result.push({ time, level: parseFloat(level.toFixed(1)) });
        }
        return result;
    };

    const formatXAxis = (tickItem) => {
        const date = new Date(tickItem);
        if (timeRange === '1h' || timeRange === '6h') {
            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }
        return date.toLocaleDateString([], { month: 'short', day: 'numeric' });
    };

    const CustomTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            return (
                <div className="custom-tooltip">
                    <p className="tooltip-date">{new Date(label).toLocaleString()}</p>
                    <p className="tooltip-value">
                        {payload[0].value} <span className="tooltip-unit">cm</span>
                    </p>
                </div>
            );
        }
        return null;
    };

    return (
        <div className="glass-panel graph-container">
            {/* Header & Controls */}
            <div className="graph-header">
                <div>
                    <h2 className="graph-title">Water Level History</h2>
                    <div className="graph-status">
                        <p className="status-text">
                            {isOffline ? 'OFFLINE SIMULATION' : 'Live Sensor Data'}
                        </p>
                        {isOffline && <span className="status-dot offline"></span>}
                        {!isOffline && <span className="status-dot online"></span>}
                    </div>
                </div>

                <div className="time-range-controls">
                    {RANGES.map((r) => (
                        <button
                            key={r.value}
                            onClick={() => setTimeRange(r.value)}
                            className={`range-btn ${timeRange === r.value ? 'active' : ''}`}
                        >
                            {r.label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Graph Area */}
            <div className="graph-area">
                {loading && <div className="graph-loading">Loading...</div>}

                {!loading && (
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                            <defs>
                                <linearGradient id="colorLevel" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4} />
                                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                            <XAxis
                                dataKey="time"
                                tickFormatter={formatXAxis}
                                stroke="#94a3b8"
                                fontSize={12}
                                tickMargin={10}
                                axisLine={false}
                                tickLine={false}
                            />
                            <YAxis
                                stroke="#94a3b8"
                                fontSize={12}
                                axisLine={false}
                                tickLine={false}
                                domain={['auto', 'auto']}
                            />
                            <Tooltip content={<CustomTooltip />} />
                            <Area
                                type="monotone"
                                dataKey="level"
                                stroke="#3b82f6"
                                strokeWidth={3}
                                fillOpacity={1}
                                fill="url(#colorLevel)"
                                animationDuration={1500}
                            />
                        </AreaChart>
                    </ResponsiveContainer>
                )}
            </div>
        </div>
    );
};

WaterLevelGraph.propTypes = {
    sensorId: PropTypes.string,
    sensorName: PropTypes.string,
};

export default WaterLevelGraph;
