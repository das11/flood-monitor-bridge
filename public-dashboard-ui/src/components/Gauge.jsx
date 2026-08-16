import React from "react";
import { motion } from "framer-motion";
import PropTypes from "prop-types";
import "./Gauge.css";

const Gauge = ({ level, thresholds, maxLevel = 1000 }) => {
    // Sensors report distance-to-water: a SMALLER reading means the water is
    // closer to the sensor (higher flood risk), so the alert direction is
    // inverted relative to a typical "higher = worse" gauge.
    const getStatusColor = (level, thresholds) => {
        if (level < thresholds.critical) return "#ef4444";
        if (level < thresholds.warn) return "#eab308";
        return "#3b82f6";
    };

    const getFillClass = (level, thresholds) => {
        if (level < thresholds.critical) return "fill-red";
        if (level < thresholds.warn) return "fill-yellow";
        return "fill-blue";
    };

    const percentage = Math.min((level / maxLevel) * 100, 100);
    const statusColor = getStatusColor(level, thresholds);

    return (
        <div className="gauge-container">
            <div className="gauge-header">
                <h3 className="gauge-title">Live Water Level</h3>
            </div>

            {/* Gauge Visual */}
            <div className="gauge-body">

                {/* Animated Liquid Fill */}
                <motion.div
                    className={`gauge-fill ${getFillClass(level, thresholds)}`}
                    initial={{ height: "0%" }}
                    animate={{ height: `${percentage}%` }}
                    transition={{
                        type: "spring",
                        stiffness: 40,
                        damping: 15
                    }}
                >
                    <div className="gauge-wave"></div>
                </motion.div>

                {/* Level Text Overlay */}
                <div className="gauge-overlay">
                    <span className="gauge-value">
                        {Math.round(level)}
                    </span>
                    <span className="gauge-unit">cm</span>
                </div>
            </div>

            {/* Threshold Indicators */}
            <div className="gauge-legend">
                <div className="legend-item">
                    <span className="legend-dot fill-blue"></span>
                    <span className="legend-label">Safe</span>
                </div>
                <div className="legend-item">
                    <span className="legend-dot fill-yellow"></span>
                    <span className="legend-label">Warn</span>
                </div>
                <div className="legend-item">
                    <span className="legend-dot fill-red"></span>
                    <span className="legend-label">Crit</span>
                </div>
            </div>

            {/* Status Badge */}
            <div className="status-display">
                <span className="status-indicator" style={{ backgroundColor: statusColor }}></span>
                <span>
                    {level < thresholds.critical ? "High Alert" : level < thresholds.warn ? "Elevated Levels" : "Normal Levels"}
                </span>
            </div>
        </div>
    );
};

Gauge.propTypes = {
    level: PropTypes.number.isRequired,
    thresholds: PropTypes.shape({
        critical: PropTypes.number,
        warn: PropTypes.number,
    }).isRequired,
    maxLevel: PropTypes.number,
};

export default Gauge;
