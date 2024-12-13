//! StatsD exporter implementation for OpenTelemetry metrics.
//!
//! This module provides functionality to export OpenTelemetry metrics to a StatsD server
//! using UDP transport. It supports both gauge and counter metrics with their associated
//! attributes converted to StatsD tags.

use std::net::UdpSocket;

use async_trait::async_trait;
use cadence::{Counted, Gauged, StatsdClient, UdpMetricSink};
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{
    data::{Gauge, Histogram, Metric, ResourceMetrics, Sum},
    exporter::PushMetricExporter,
    MetricResult, Temporality,
};
use thiserror::Error;
use tracing::{debug, warn};

/// Represents errors that can occur during StatsD export operations
#[derive(Error, Debug)]
pub enum StatsdError {
    /// Indicates a failure to bind to a UDP socket
    #[error("Failed to bind UDP socket: {0}")]
    SocketBind(#[from] std::io::Error),

    /// Indicates a failure in creating the StatsD metric sink
    #[error("Failed to create metric sink: {0}")]
    SinkCreation(#[from] cadence::MetricError),
}

/// Exports OpenTelemetry metrics to a StatsD server.
///
/// Handles conversion of OpenTelemetry metrics to StatsD format, including:
/// - Gauge metrics
/// - Counter metrics (as sums)
/// - Attribute conversion to StatsD tags
#[derive(Debug)]
pub struct StatsdExporter {
    client: StatsdClient,
}

impl StatsdExporter {
    /// Creates a new StatsD exporter instance.
    ///
    /// # Arguments
    /// * `host` - The host address of the StatsD server (e.g., "localhost")
    ///
    /// # Returns
    /// A Result containing either the new StatsdExporter or a StatsdError
    ///
    /// # Example
    /// ```no_run
    /// use opentelemetry_statsd_exporter::StatsdExporter;
    ///
    /// let exporter = StatsdExporter::new("localhost".to_string()).expect("Failed to create exporter");
    /// ```
    pub fn new(host: String) -> Result<Self, StatsdError> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_nonblocking(true)?;

        let sink = UdpMetricSink::from((host.as_str(), 8125), socket)?;
        let client = StatsdClient::from_sink("balances_api", sink);

        Ok(Self { client })
    }
}

#[async_trait]
impl PushMetricExporter for StatsdExporter {
    /// Exports OpenTelemetry metrics to StatsD.
    ///
    /// Converts OpenTelemetry metrics to StatsD format, maintaining:
    /// - Metric names
    /// - Metric values
    /// - Attributes as StatsD tags
    ///
    /// # Arguments
    /// * `metrics` - OpenTelemetry ResourceMetrics to export
    ///
    /// # Returns
    /// A MetricResult indicating success or failure of the export operation
    async fn export(&self, metrics: &mut ResourceMetrics) -> MetricResult<()> {
        for scope_metrics in &metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                self.export_metric(metric)?;
            }
        }
        Ok(())
    }

    async fn force_flush(&self) -> MetricResult<()> {
        // UDP is fire-and-forget, no flush needed
        Ok(())
    }

    fn shutdown(&self) -> MetricResult<()> {
        // No special cleanup needed
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        // Most StatsD implementations expect delta values
        Temporality::Delta
    }
}

impl StatsdExporter {
    /// Helper method to export a single metric
    fn export_metric(&self, metric: &Metric) -> MetricResult<()> {
        if let Some(gauge) = metric.data.as_any().downcast_ref::<Gauge<u64>>() {
            self.export_gauge(metric.name.to_string().as_str(), gauge)
        } else if let Some(sum) = metric.data.as_any().downcast_ref::<Sum<u64>>() {
            self.export_sum(metric.name.to_string().as_str(), sum)
        } else if let Some(histogram) = metric.data.as_any().downcast_ref::<Histogram<u64>>() {
            self.export_histogram(metric.name.to_string().as_str(), histogram)
        } else {
            // Means you have to implement the conversion for this metric type
            warn!(
                   "Unsupported metric type for metric '{}'. Only u64 gauges, counters and histograms are supported.",
                   metric.name
               );
            Ok(())
        }
    }

    fn export_gauge(&self, name: &str, gauge: &Gauge<u64>) -> MetricResult<()> {
        for datapoint in &gauge.data_points {
            let tags = self.convert_attributes_to_tags(&datapoint.attributes);
            let mut builder = self.client.gauge_with_tags(name, datapoint.value);

            for (key, value) in &tags {
                builder = builder.with_tag(key.as_str(), value.as_str());
            }

            debug!(
                "Sending gauge metric: {} = {} with attributes: {:?}",
                name, datapoint.value, datapoint.attributes
            );

            builder.send();
        }
        Ok(())
    }

    fn export_sum(&self, name: &str, sum: &Sum<u64>) -> MetricResult<()> {
        for datapoint in &sum.data_points {
            let tags = self.convert_attributes_to_tags(&datapoint.attributes);
            let mut builder = self.client.count_with_tags(name, datapoint.value);

            for (key, value) in &tags {
                builder = builder.with_tag(key.as_str(), value.as_str());
            }

            debug!(
                "Sending count metric: {} = {} with attributes: {:?}",
                name, datapoint.value, datapoint.attributes
            );

            builder.send();
        }
        Ok(())
    }

    fn export_histogram(&self, name: &str, histogram: &Histogram<u64>) -> MetricResult<()> {
        for datapoint in &histogram.data_points {
            let tags = self.convert_attributes_to_tags(&datapoint.attributes);

            // Export min value
            if let Some(min) = datapoint.min {
                let min_name = format!("{}.min", name);
                let mut min_builder = self.client.gauge_with_tags(&min_name, min);
                for (key, value) in &tags {
                    min_builder = min_builder.with_tag(key.as_str(), value.as_str());
                }
                min_builder.send();
            }

            // Export max value
            if let Some(max) = datapoint.max {
                let max_name = format!("{}.max", name);
                let mut max_builder = self.client.gauge_with_tags(&max_name, max);
                for (key, value) in &tags {
                    max_builder = max_builder.with_tag(key.as_str(), value.as_str());
                }
                max_builder.send();
            }

            // Export count value
            let count_name = format!("{}.count", name);
            let mut count_builder = self.client.count_with_tags(&count_name, datapoint.count);
            for (key, value) in &tags {
                count_builder = count_builder.with_tag(key.as_str(), value.as_str());
            }
            count_builder.send();

            // Calculate and export p95
            if let Some(p95) =
                calculate_percentile(&datapoint.bucket_counts, &datapoint.bounds, 0.95)
            {
                let p95_name = format!("{}.p95", name);
                let mut p95_builder = self.client.gauge_with_tags(&p95_name, p95);
                for (key, value) in &tags {
                    p95_builder = p95_builder.with_tag(key.as_str(), value.as_str());
                }
                p95_builder.send();
            }

            // Export average value
            let (sum, count) = (datapoint.sum, datapoint.count);
            if count > 0 {
                let avg = sum / count;
                let avg_name = format!("{}.avg", name);
                let mut avg_builder = self.client.gauge_with_tags(&avg_name, avg);
                for (key, value) in &tags {
                    avg_builder = avg_builder.with_tag(key.as_str(), value.as_str());
                }
                avg_builder.send();
            }

            debug!(
                "Sending histogram metric: {} min={:?} max={:?} with attributes: {:?}",
                name, datapoint.min, datapoint.max, datapoint.attributes
            );
        }
        Ok(())
    }

    fn convert_attributes_to_tags(&self, attributes: &[KeyValue]) -> Vec<(String, String)> {
        attributes
            .iter()
            .map(|attr| (attr.key.to_string(), attr.value.to_string()))
            .collect()
    }
}
/// Calculate percentile from histogram buckets
fn calculate_percentile(bucket_counts: &[u64], bounds: &[f64], percentile: f64) -> Option<u64> {
    let total: u64 = bucket_counts.iter().sum();
    if total == 0 {
        return None;
    }

    let target = (percentile * total as f64).ceil() as u64;
    let mut count_so_far = 0;

    for (i, &bucket_count) in bucket_counts.iter().enumerate() {
        count_so_far += bucket_count;
        if count_so_far >= target {
            // For the last bucket, use the upper bound
            if i == bounds.len() {
                return Some(bounds[bounds.len() - 1] as u64);
            }
            // For other buckets, use linear interpolation
            return Some(bounds[i] as u64);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_new_exporter() {
        let result = StatsdExporter::new("localhost".to_string());
        assert!(result.is_ok());
    }
    //
    // Error cases
    #[test]
    fn test_invalid_host() {
        let result = StatsdExporter::new("256.256.256.256".to_string());
        assert!(result.is_err());
        match result {
            Err(StatsdError::SinkCreation(_)) => (),
            _ => panic!("Expected SocketBind error, got{:?}", result),
        }
    }
}
