import React, { useState, useEffect, useRef, useCallback } from 'react';
import { fetchSensorImages } from '../services/influxService';
import PropTypes from 'prop-types';
import './ImageTimeline.css';

const RANGES = [
    { label: '1H', value: '1h' },
    { label: '6H', value: '6h' },
    { label: '24H', value: '24h' },
    { label: '7D', value: '7d' },
    { label: '30D', value: '30d' },
];

const ImageTimeline = ({ sensorId, imageViews }) => {
    const [images, setImages] = useState([]);
    const [activeIndex, setActiveIndex] = useState(0);
    const [activeView, setActiveView] = useState(imageViews?.[0] || 'front');
    const [timeRange, setTimeRange] = useState('24h');
    const [loading, setLoading] = useState(true);
    const [lightboxOpen, setLightboxOpen] = useState(false);
    const thumbnailStripRef = useRef(null);

    // Fetch images when sensor, view, or time range changes
    useEffect(() => {
        let isMounted = true;

        const loadImages = async () => {
            setLoading(true);
            if (!sensorId) return;

            const result = await fetchSensorImages(sensorId, activeView, timeRange);

            if (isMounted) {
                if (result && result.length > 0) {
                    setImages(result);
                    setActiveIndex(result.length - 1); // Start at latest
                } else {
                    setImages([]);
                    setActiveIndex(0);
                }
                setLoading(false);
            }
        };

        loadImages();

        // Refresh every 2 minutes
        const interval = setInterval(loadImages, 120000);
        return () => {
            isMounted = false;
            clearInterval(interval);
        };
    }, [sensorId, activeView, timeRange]);

    // Scroll active thumbnail into view
    useEffect(() => {
        if (thumbnailStripRef.current && images.length > 0) {
            const activeThumb = thumbnailStripRef.current.children[activeIndex];
            if (activeThumb) {
                activeThumb.scrollIntoView({
                    behavior: 'smooth',
                    block: 'nearest',
                    inline: 'center'
                });
            }
        }
    }, [activeIndex, images]);

    // Keyboard navigation
    useEffect(() => {
        const handleKeyDown = (e) => {
            if (lightboxOpen && e.key === 'Escape') {
                setLightboxOpen(false);
                return;
            }
            if (e.key === 'ArrowLeft') {
                setActiveIndex(prev => Math.max(0, prev - 1));
            } else if (e.key === 'ArrowRight') {
                setActiveIndex(prev => Math.min(images.length - 1, prev + 1));
            }
        };

        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [images.length, lightboxOpen]);

    const formatTime = useCallback((timestamp) => {
        const date = new Date(timestamp);
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }, []);

    const formatFullTimestamp = useCallback((timestamp) => {
        const date = new Date(timestamp);
        return date.toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
        }) + ' · ' + date.toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }, []);

    // Generate timeline labels (evenly spaced time labels)
    const getTimelineLabels = useCallback(() => {
        if (images.length < 2) return [];
        const first = images[0].time;
        const last = images[images.length - 1].time;
        const labelCount = Math.min(6, images.length);
        const labels = [];
        for (let i = 0; i < labelCount; i++) {
            const t = first + (last - first) * (i / (labelCount - 1));
            labels.push(formatTime(t));
        }
        return labels;
    }, [images, formatTime]);

    const activeImage = images[activeIndex];

    // If no image views configured, don't render
    if (!imageViews || imageViews.length === 0) return null;

    // Loading state
    if (loading) {
        return (
            <div className="glass-panel image-timeline">
                <div className="image-timeline-loading">
                    <span className="loading-spinner"></span>
                    Loading camera feed...
                </div>
            </div>
        );
    }

    // Empty state
    if (images.length === 0) {
        return (
            <div className="glass-panel image-timeline">
                <div className="image-timeline-header">
                    <div className="image-timeline-title-block">
                        <h2 className="image-timeline-title">
                            <span className="camera-icon">📷</span>
                            Sensor Camera Feed
                        </h2>
                    </div>
                    <div className="image-timeline-controls">
                        <div className="view-tabs">
                            {imageViews.map(view => (
                                <button
                                    key={view}
                                    className={`view-tab ${activeView === view ? 'active' : ''}`}
                                    onClick={() => setActiveView(view)}
                                >
                                    {view}
                                </button>
                            ))}
                        </div>
                        <div className="time-range-controls">
                            {RANGES.map(r => (
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
                </div>
                <div className="image-timeline-empty">
                    <div className="empty-icon">📸</div>
                    <h3>No images available</h3>
                    <p>No camera images found for the selected time range and view.</p>
                </div>
            </div>
        );
    }

    return (
        <>
            <div className="glass-panel image-timeline">
                {/* Header */}
                <div className="image-timeline-header">
                    <div className="image-timeline-title-block">
                        <h2 className="image-timeline-title">
                            <span className="camera-icon">📷</span>
                            Sensor Camera Feed
                        </h2>
                        <p className="image-timeline-subtitle">
                            {activeView} view · {images.length} images
                        </p>
                    </div>
                    <div className="image-timeline-controls">
                        {imageViews.length > 1 && (
                            <div className="view-tabs">
                                {imageViews.map(view => (
                                    <button
                                        key={view}
                                        className={`view-tab ${activeView === view ? 'active' : ''}`}
                                        onClick={() => setActiveView(view)}
                                    >
                                        {view}
                                    </button>
                                ))}
                            </div>
                        )}
                        <div className="time-range-controls">
                            {RANGES.map(r => (
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
                </div>

                {/* Main Image Display */}
                <div className="image-display-area">
                    {activeImage && (
                        <img
                            src={activeImage.imageUrl}
                            alt={`${activeView} view at ${formatFullTimestamp(activeImage.time)}`}
                            onClick={() => setLightboxOpen(true)}
                            style={{ cursor: 'zoom-in' }}
                        />
                    )}

                    {/* Navigation arrows */}
                    <button
                        className="image-nav-btn prev"
                        onClick={() => setActiveIndex(prev => Math.max(0, prev - 1))}
                        disabled={activeIndex === 0}
                        aria-label="Previous image"
                    >
                        ‹
                    </button>
                    <button
                        className="image-nav-btn next"
                        onClick={() => setActiveIndex(prev => Math.min(images.length - 1, prev + 1))}
                        disabled={activeIndex === images.length - 1}
                        aria-label="Next image"
                    >
                        ›
                    </button>

                    {/* Overlay info */}
                    {activeImage && (
                        <div className="image-info-overlay">
                            <span className="image-timestamp">
                                {formatFullTimestamp(activeImage.time)}
                            </span>
                            <span className="image-counter">
                                {activeIndex + 1} / {images.length}
                            </span>
                        </div>
                    )}
                </div>

                {/* Timeline Scrubber */}
                <div className="timeline-scrubber">
                    <div
                        className="timeline-track"
                        onClick={(e) => {
                            const rect = e.currentTarget.getBoundingClientRect();
                            const pct = (e.clientX - rect.left) / rect.width;
                            const idx = Math.round(pct * (images.length - 1));
                            setActiveIndex(Math.max(0, Math.min(images.length - 1, idx)));
                        }}
                    >
                        <div
                            className="timeline-progress"
                            style={{
                                width: images.length > 1
                                    ? `${(activeIndex / (images.length - 1)) * 100}%`
                                    : '100%'
                            }}
                        />
                        <div className="timeline-dots">
                            {images.map((img, idx) => (
                                <div
                                    key={idx}
                                    className={`timeline-dot ${idx === activeIndex ? 'active' : ''}`}
                                    style={{
                                        left: images.length > 1
                                            ? `${(idx / (images.length - 1)) * 100}%`
                                            : '50%'
                                    }}
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        setActiveIndex(idx);
                                    }}
                                    title={formatTime(img.time)}
                                />
                            ))}
                        </div>
                    </div>
                    <div className="timeline-labels">
                        {getTimelineLabels().map((label, i) => (
                            <span key={i} className="timeline-label">{label}</span>
                        ))}
                    </div>
                </div>

                {/* Thumbnail Strip */}
                <div className="thumbnail-strip" ref={thumbnailStripRef}>
                    {images.map((img, idx) => (
                        <div
                            key={idx}
                            className={`thumbnail-item ${idx === activeIndex ? 'active' : ''}`}
                            onClick={() => setActiveIndex(idx)}
                        >
                            <img
                                src={img.imageUrl}
                                alt={`Thumbnail ${formatTime(img.time)}`}
                                loading="lazy"
                            />
                            <span className="thumbnail-time">
                                {formatTime(img.time)}
                            </span>
                        </div>
                    ))}
                </div>
            </div>

            {/* Lightbox */}
            {lightboxOpen && activeImage && (
                <div
                    className="image-lightbox-overlay"
                    onClick={() => setLightboxOpen(false)}
                >
                    <img
                        src={activeImage.imageUrl}
                        alt={`${activeView} view fullscreen`}
                        onClick={(e) => e.stopPropagation()}
                    />
                    <button
                        className="lightbox-close"
                        onClick={() => setLightboxOpen(false)}
                        aria-label="Close lightbox"
                    >
                        ✕
                    </button>
                </div>
            )}
        </>
    );
};

ImageTimeline.propTypes = {
    sensorId: PropTypes.string,
    imageViews: PropTypes.arrayOf(PropTypes.string),
};

export default ImageTimeline;
