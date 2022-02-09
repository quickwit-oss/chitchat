// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::Duration;
#[cfg(not(test))]
use std::time::Instant;

#[cfg(test)]
use mock_instant::Instant;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// A phi accrual failure detector implementation.
pub struct FailureDetector {
    /// Heartbeat samples for each node.
    node_samples: RwLock<HashMap<String, SamplingWindow>>,
    /// Failure detector configuration.
    config: FailureDetectorConfig,
    /// Denotes live nodes.
    live_nodes: HashSet<String>,
}

impl FailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            node_samples: RwLock::new(HashMap::new()),
            config,
            live_nodes: HashSet::new(),
        }
    }

    /// Reports node heartbeat.
    pub fn report_heartbeat(&mut self, node_id: &str) {
        debug!(node_id = node_id, "Reporting node heartbeat.");
        let mut node_samples = self.node_samples.write().unwrap();
        let heartbeat_window = node_samples.entry(node_id.to_string()).or_insert_with(|| {
            SamplingWindow::new(
                self.config.sampling_window_size,
                self.config.max_interval,
                self.config.initial_interval,
            )
        });
        heartbeat_window.report_heartbeat();
    }

    /// Marks or unmarks a node as dead.
    pub fn update_node_liveliness(&mut self, node_id: &str) {
        let phi = self.phi(node_id);
        debug!(node_id = node_id, phi = phi, "updating node liveliness");
        if phi > self.config.phi_threshold {
            self.live_nodes.remove(node_id);
            // Remove current sampling window so that when the node
            // comes back online, we start with a fresh sampling window.
            self.remove(node_id);
        } else {
            self.live_nodes.insert(node_id.to_string());
        }
    }

    /// Returns a list of living nodes.
    pub fn living_nodes(&self) -> impl Iterator<Item = &str> {
        self.live_nodes.iter().map(|node_id| node_id.as_str())
    }

    /// Removes a node.
    fn remove(&mut self, node_id: &str) {
        self.node_samples.write().unwrap().remove(node_id);
    }

    /// Returns the current phi value of a node.
    fn phi(&mut self, node_id: &str) -> f64 {
        self.node_samples
            .read()
            .unwrap()
            .get(node_id)
            .map_or(self.config.phi_threshold - 1.0, |sampling_window| {
                sampling_window.phi()
            })
    }
}

/// The failure detector config struct.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FailureDetectorConfig {
    /// Phi threshold value above which a node is flagged as faulty.
    pub phi_threshold: f64,
    /// Sampling window size
    pub sampling_window_size: usize,
    /// Heartbeat longer than this will be droped.
    pub max_interval: Duration,
    /// Initial interval used on stratup when no previous heartbeat exists.  
    pub initial_interval: Duration,
}

impl FailureDetectorConfig {
    pub fn new(
        phi_threshold: f64,
        sampling_window_size: usize,
        max_interval: Duration,
        initial_interval: Duration,
    ) -> Self {
        Self {
            phi_threshold,
            sampling_window_size,
            max_interval,
            initial_interval,
        }
    }
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            sampling_window_size: 1000,
            max_interval: Duration::from_secs(10),
            initial_interval: Duration::from_secs(5),
        }
    }
}

/// A fixed-sized window that keeps track of the most recent heartbeat arrival intervals.
#[derive(Debug)]
struct SamplingWindow {
    /// The set of collected intervals.
    intervals: BoundedArrayStats,
    /// Last heartbeat reported time.
    last_hearbeat: Option<Instant>,
    /// Heartbeat intervals greater than this value are ignored.
    max_interval: Duration,
    /// The initial interval on startup.
    initial_interval: Duration,
}

impl SamplingWindow {
    // Construct a new instance.
    pub fn new(window_size: usize, max_interval: Duration, initial_interval: Duration) -> Self {
        Self {
            intervals: BoundedArrayStats::new(window_size),
            last_hearbeat: None,
            max_interval,
            initial_interval,
        }
    }

    /// Reports a heartbeat.
    pub fn report_heartbeat(&mut self) {
        if let Some(last_value) = &self.last_hearbeat {
            let interval = last_value.elapsed();
            if interval <= self.max_interval {
                self.intervals.append(interval.as_secs_f64());
            }
        } else {
            self.intervals.append(self.initial_interval.as_secs_f64());
        };
        self.last_hearbeat = Some(Instant::now());
    }

    /// Computes the sampling window's phi value.
    pub fn phi(&self) -> f64 {
        // Ensure we don't call before any sample arrival.
        assert!(self.intervals.mean() > 0.0 && self.last_hearbeat.is_some());

        let elapsed_time = self.last_hearbeat.unwrap().elapsed().as_secs_f64();
        elapsed_time / self.intervals.mean()
    }
}

/// An array that retains a fixed number of streaming values.
#[derive(Debug)]
struct BoundedArrayStats {
    /// The values.
    data: Vec<f64>,
    /// Number of accumulated values.
    size: usize,
    /// Is the values array filled?
    is_filled: bool,
    /// Position of the index within the values array.
    index: usize,
    /// The accumulated sum of values.
    sum: f64,
    /// The accumulated mean of values.
    mean: f64,
}

impl BoundedArrayStats {
    pub fn new(size: usize) -> Self {
        Self {
            data: vec![0.0; size],
            size,
            is_filled: false,
            index: 0,
            sum: 0.0,
            mean: 0.0,
        }
    }

    /// Returns the mean.
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Appends a new value and updates the statistics.
    pub fn append(&mut self, interval: f64) {
        if self.index == self.size {
            self.is_filled = true;
            self.index = 0;
        }

        if self.is_filled {
            self.sum -= self.data[self.index];
        }
        self.sum += interval;

        self.data[self.index] = interval;
        self.index += 1;

        self.mean = self.sum / self.len() as f64;
    }

    fn len(&self) -> usize {
        if self.is_filled {
            return self.size;
        }
        self.index
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mock_instant::MockClock;
    use rand::prelude::*;

    use super::{BoundedArrayStats, SamplingWindow};
    use crate::failure_detector::{FailureDetector, FailureDetectorConfig};

    #[test]
    fn test_failure_detector() {
        let mut rng = rand::thread_rng();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());

        let intervals_choices = vec![1u64, 2];
        let node_ids_choices = vec!["node-1", "node-2", "node-3"];
        for _ in 0..=2000 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            let node_id = node_ids_choices.choose(&mut rng).unwrap();
            MockClock::advance(Duration::from_secs(*time_offset));
            failure_detector.report_heartbeat(node_id);
        }

        for node_id in &node_ids_choices {
            failure_detector.update_node_liveliness(node_id);
        }

        let mut living_nodes = failure_detector.living_nodes().collect::<Vec<_>>();
        living_nodes.sort();

        assert_eq!(living_nodes, vec!["node-1", "node-2", "node-3"]);
    }

    #[test]
    fn test_failure_detector_node_state_from_live_to_down_to_live() {
        let mut rng = rand::thread_rng();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let intervals_choices = vec![1u64, 2];

        for _ in 0..=2000 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            MockClock::advance(Duration::from_secs(*time_offset));
            failure_detector.report_heartbeat("node-1");
        }

        failure_detector.update_node_liveliness("node-1");
        assert_eq!(
            failure_detector.living_nodes().collect::<Vec<&str>>(),
            vec!["node-1"]
        );

        // Check node-1 is down (stop reporting heartbeat).
        MockClock::advance(Duration::from_secs(20));
        failure_detector.update_node_liveliness("node-1");
        assert_eq!(
            failure_detector.living_nodes().collect::<Vec<&str>>(),
            Vec::<&str>::new()
        );

        // Check node-1 is back up (resume reporting heartbeat).
        for _ in 0..=500 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            MockClock::advance(Duration::from_secs(*time_offset));
            failure_detector.report_heartbeat("node-1");
        }
        failure_detector.update_node_liveliness("node-1");
        assert_eq!(
            failure_detector.living_nodes().collect::<Vec<&str>>(),
            vec!["node-1"]
        );
    }

    #[test]
    fn test_sampling_window() {
        let mut sampling_window =
            SamplingWindow::new(10, Duration::from_secs(5), Duration::from_secs(2));
        sampling_window.report_heartbeat();

        MockClock::advance(Duration::from_secs(3));
        sampling_window.report_heartbeat();

        // Now intervals window is: [2.0, 3.0].
        let mean = (2.0 + 3.0) / 2.0;

        // 0s elapsed since last reported heartbeat.
        assert_eq!(sampling_window.phi(), 0.0 / mean);

        // 1s elapsed since last reported heartbeat.
        MockClock::advance(Duration::from_secs(1));
        assert_eq!(sampling_window.phi(), 1.0 / mean);

        // Check reported heartbeat later than max_interval is ignore.
        MockClock::advance(Duration::from_secs(5));
        sampling_window.report_heartbeat();
        MockClock::advance(Duration::from_secs(2));
        assert_eq!(
            sampling_window.phi(),
            2.0 / mean,
            "Mean value should not change."
        );
    }

    #[test]
    fn test_bounded_array_stats() {
        let mut bounded_array = BoundedArrayStats::new(10);
        for i in 1..10 {
            bounded_array.append(i as f64);
        }
        assert_eq!(bounded_array.index, 9);
        assert_eq!(bounded_array.len(), 9);
        assert!(!bounded_array.is_filled);
        assert_eq!(bounded_array.mean(), 5.0f64);

        for i in 10..14 {
            bounded_array.append(i as f64);
        }
        assert_eq!(bounded_array.index, 3);
        assert_eq!(bounded_array.len(), 10);
        assert!(bounded_array.is_filled);
        assert_eq!(bounded_array.mean(), 8.5f64);
    }
}
