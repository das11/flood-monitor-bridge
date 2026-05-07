import React from "react";
import PropTypes from "prop-types";
import { ChevronDown } from "lucide-react";
import "./SensorPicker.css";

const SensorPicker = ({ sensors, selectedId, onSelect }) => {
    return (
        <div className="sensor-picker-card">
            <label className="picker-label">Active Monitoring Node</label>
            <div className="custom-select-wrapper">
                <select
                    className="native-select"
                    value={selectedId}
                    onChange={(e) => {
                        const selectedVal = e.target.value;
                        const sensor = sensors.find(s => s.id === selectedVal);
                        if (sensor && sensor.external_url) {
                            window.open(sensor.external_url, "_blank", "noopener,noreferrer");
                        } else {
                            onSelect(selectedVal);
                        }
                    }}
                >
                    {sensors.map((sensor) => (
                        <option key={sensor.id} value={sensor.id}>
                            {sensor.name}
                        </option>
                    ))}
                </select>
                <div className="select-icon">
                    <ChevronDown size={18} />
                </div>
            </div>
        </div>
    );
};

SensorPicker.propTypes = {
    sensors: PropTypes.arrayOf(
        PropTypes.shape({
            id: PropTypes.string.isRequired,
            name: PropTypes.string.isRequired,
        })
    ).isRequired,
    selectedId: PropTypes.string.isRequired,
    onSelect: PropTypes.func.isRequired,
};

export default SensorPicker;
