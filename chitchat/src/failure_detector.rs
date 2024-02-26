use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::debug;

use crate::ChitchatId;

/// A phi accrual failure detector implementation.
pub struct FailureDetector {
    /// Heartbeat samples for each node.
    node_samples: HashMap<ChitchatId, SamplingWindow>,
    /// Failure detector configuration.
    config: FailureDetectorConfig,
    /// Denotes live nodes.
    live_nodes: HashSet<ChitchatId>,
    /// Denotes dead nodes.
    dead_nodes: HashMap<ChitchatId, Instant>,
}

impl FailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            node_samples: HashMap::new(),
            config,
            live_nodes: HashSet::new(),
            dead_nodes: HashMap::new(),
        }
    }

    /// Reports node heartbeat.
    pub fn report_heartbeat(&mut self, chitchat_id: &ChitchatId) {
        debug!(node_id=%chitchat_id.node_id, "reporting node heartbeat.");
        self.node_samples
            .entry(chitchat_id.clone())
            .or_insert_with(|| {
                SamplingWindow::new(
                    self.config.sampling_window_size,
                    self.config.max_interval,
                    self.config.initial_interval,
                )
            })
            .report_heartbeat();
    }

    /// Marks the node as dead or alive based on the current phi value.
    pub fn update_node_liveness(&mut self, chitchat_id: &ChitchatId) {
        let phi_opt = self.phi(chitchat_id);
        let is_alive = self
            .phi(chitchat_id)
            .map(|phi| phi <= self.config.phi_threshold)
            .unwrap_or(false);
        debug!(node_id=%chitchat_id.node_id, phi=?phi_opt, is_alive=is_alive, "computing node liveness");
        if is_alive {
            self.live_nodes.insert(chitchat_id.clone());
            self.dead_nodes.remove(chitchat_id);
        } else {
            self.live_nodes.remove(chitchat_id);
            if !self.dead_nodes.contains_key(chitchat_id) {
                self.dead_nodes.insert(chitchat_id.clone(), Instant::now());
            }
            // Remove all samples, so that when the node
            // comes back online, we start with a fresh sampling window.
            if let Some(node_sample) = self.node_samples.get_mut(chitchat_id) {
                node_sample.reset();
            }
        }
    }

    /// Removes and returns the list of garbage collectible nodes.
    pub fn garbage_collect(&mut self) -> Vec<ChitchatId> {
        let mut garbage_collected_nodes = Vec::new();
        let now = Instant::now();
        for (chitchat_id, &time_of_death) in &self.dead_nodes {
            if now >= time_of_death + self.config.dead_node_grace_period {
                garbage_collected_nodes.push(chitchat_id.clone())
            }
        }
        for chitchat_id in &garbage_collected_nodes {
            self.dead_nodes.remove(chitchat_id);
            self.node_samples.remove(chitchat_id);
        }
        garbage_collected_nodes
    }

    /// Returns the list of nodes considered live by the failure detector.
    pub fn live_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        self.live_nodes.iter()
    }

    /// Returns the list of nodes considered dead by the failure detector.
    pub fn dead_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        self.dead_nodes.keys()
    }

    /// Returns the list of nodes considered dead by the failure detector.
    pub fn scheduled_for_deletion_nodes(&self) -> impl Iterator<Item = &ChitchatId> {
        let now = Instant::now();
        let half_dead_node_grace_period = self.config.dead_node_grace_period.div_f32(2.0f32);
        // Note: we can't just compute the threshold now - half_dead_node_grace_period, because it
        // would underflow on some platform (MacOS).
        self.dead_nodes
            .iter()
            .filter_map(move |(chitchat_id, time_of_death)| {
                if *time_of_death + half_dead_node_grace_period < now {
                    Some(chitchat_id)
                } else {
                    None
                }
            })
    }

    /// Returns the current phi value of a node.
    ///
    /// If we have received less than 2 heartbeat, `phi()` returns `None`.
    fn phi(&mut self, chitchat_id: &ChitchatId) -> Option<f64> {
        self.node_samples.get(chitchat_id)?.phi()
    }
}

/// The failure detector config struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureDetectorConfig {
    /// Phi threshold value above which a node is flagged as faulty.
    pub phi_threshold: f64,
    /// Sampling window size
    pub sampling_window_size: usize,
    /// Heartbeat longer than this will be dropped.
    pub max_interval: Duration,
    /// Initial interval used on startup when no previous heartbeat exists.
    pub initial_interval: Duration,
    /// Threshold period after which dead node can be removed from the cluster.
    pub dead_node_grace_period: Duration,
}

impl FailureDetectorConfig {
    pub fn new(
        phi_threshold: f64,
        sampling_window_size: usize,
        max_interval: Duration,
        initial_interval: Duration,
        dead_node_grace_period: Duration,
    ) -> Self {
        Self {
            phi_threshold,
            sampling_window_size,
            max_interval,
            initial_interval,
            dead_node_grace_period,
        }
    }
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            sampling_window_size: 1_000,
            max_interval: Duration::from_secs(10),
            initial_interval: Duration::from_secs(5),
            dead_node_grace_period: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }
}

#[derive(Debug)]
struct AdditiveSmoothing {
    prior_mean: f64,
    prior_weight: f64,
}

impl AdditiveSmoothing {
    fn compute_mean(&self, len: NonZeroUsize, sum: f64) -> f64 {
        (sum + self.prior_weight * self.prior_mean) / (len.get() as f64 + self.prior_weight)
    }
}

/// A fixed-sized window that keeps track of the most recent heartbeat arrival intervals.
#[derive(Debug)]
struct SamplingWindow {
    /// The set of collected intervals.
    intervals: BoundedArrayStats,
    /// Last heartbeat reported time.
    last_heartbeat: Option<Instant>,
    /// Heartbeat intervals greater than this value are ignored.
    max_interval: Duration,
    /// We may not have many intervals in the beginning.
    /// For this reason we use additive smoothing to make sure we are
    /// lenient on the first few intervals, and we don't have nodes flapping from
    /// life to death.
    additive_smoothing: AdditiveSmoothing,
}

impl SamplingWindow {
    // Construct a new instance.
    pub fn new(window_size: usize, max_interval: Duration, prior_interval: Duration) -> Self {
        let additive_smoothing = AdditiveSmoothing {
            prior_mean: prior_interval.as_secs_f64(),
            prior_weight: 5.0f64,
        };
        SamplingWindow {
            intervals: BoundedArrayStats::with_capacity(window_size),
            last_heartbeat: None,
            max_interval,
            additive_smoothing,
        }
    }

    /// Reports a heartbeat.
    pub fn report_heartbeat(&mut self) {
        let now = Instant::now();
        if let Some(last_value) = self.last_heartbeat {
            let interval = now.duration_since(last_value);
            if interval <= self.max_interval {
                self.intervals.append(interval.as_secs_f64());
            }
        } else {
            // This is our first heartbeat.
            // No way to compute an interval.
            // This is fine.
        }
        self.last_heartbeat = Some(now);
    }

    /// Forget about all previous intervals.
    pub fn reset(&mut self) {
        self.intervals.clear();
    }

    /// Computes the sampling window's phi value.
    /// Returns `None` if have not received two heartbeat yet.
    pub fn phi(&self) -> Option<f64> {
        // We avoid computing phi if we have only received one heartbeat.
        // It could be data from an old dead node after all.
        let len_non_zero = NonZeroUsize::new(self.intervals.len())?;
        let sum = self.intervals.sum();
        let last_heartbeat = self.last_heartbeat?;
        let interval_mean = self.additive_smoothing.compute_mean(len_non_zero, sum);
        let elapsed_time = last_heartbeat.elapsed().as_secs_f64();
        Some(elapsed_time / interval_mean)
    }
}

/// An array that retains a fixed number of streaming values.
#[derive(Debug)]
struct BoundedArrayStats {
    /// The values.
    values: Box<[f64]>,
    /// Is the values array filled?
    is_filled: bool,
    /// Position of the next value to be written in the values array.
    index: usize,
    /// The accumulated sum of values.
    sum: f64,
}

impl BoundedArrayStats {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: vec![0.0; capacity].into_boxed_slice(),
            is_filled: false,
            index: 0,
            sum: 0.0,
        }
    }

    /// Returns the mean.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Appends a new value and updates the statistics.
    pub fn append(&mut self, interval: f64) {
        if self.is_filled {
            self.sum -= self.values[self.index];
        }
        self.values[self.index] = interval;
        self.sum += interval;
        if self.index == self.values.len() - 1 {
            self.is_filled = true;
            self.index = 0;
        } else {
            self.index += 1;
        }
    }

    pub fn clear(&mut self) {
        self.index = 0;
        self.is_filled = false;
        self.sum = 0f64;
    }

    fn len(&self) -> usize {
        if self.is_filled {
            return self.values.len();
        }
        self.index
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use rand::prelude::*;

    use super::{BoundedArrayStats, SamplingWindow};
    use crate::failure_detector::{FailureDetector, FailureDetectorConfig};
    use crate::ChitchatId;

    impl FailureDetector {
        pub fn contains_node(&self, chitchat_id: &ChitchatId) -> bool {
            self.node_samples.contains_key(chitchat_id)
        }
    }

    #[test]
    fn test_failure_detector_does_not_see_a_node_as_alive_with_a_single_heartbeat() {
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let chitchat_id = ChitchatId::for_local_test(10_001);
        failure_detector.report_heartbeat(&chitchat_id);
        failure_detector.update_node_liveness(&chitchat_id);
        let dead_nodes: Vec<&ChitchatId> = failure_detector.dead_nodes().collect();
        assert_eq!(dead_nodes.len(), 1);
        assert!(failure_detector.live_nodes().next().is_none());
    }

    #[tokio::test]
    async fn test_failure_detector() {
        tokio::time::pause();
        let mut rng = rand::thread_rng();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());

        let intervals_choices = [1u64, 2];
        let chitchat_ids_choices = vec![
            ChitchatId::for_local_test(10_001),
            ChitchatId::for_local_test(10_002),
            ChitchatId::for_local_test(10_003),
        ];
        for _ in 0..=2000 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            let chitchat_id = chitchat_ids_choices.choose(&mut rng).unwrap();
            tokio::time::advance(Duration::from_secs(*time_offset)).await;
            failure_detector.report_heartbeat(chitchat_id);
        }

        for chitchat_id in &chitchat_ids_choices {
            failure_detector.update_node_liveness(chitchat_id);
        }

        let mut live_nodes = failure_detector
            .live_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        live_nodes.sort_unstable();
        assert_eq!(live_nodes, vec!["node-10001", "node-10002", "node-10003"]);
        assert_eq!(failure_detector.garbage_collect(), Vec::new());

        // stop reporting heartbeat for few seconds
        tokio::time::advance(Duration::from_secs(50)).await;
        for chitchat_id in &chitchat_ids_choices {
            failure_detector.update_node_liveness(chitchat_id);
        }
        let mut dead_nodes = failure_detector
            .dead_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        dead_nodes.sort_unstable();
        assert_eq!(dead_nodes, vec!["node-10001", "node-10002", "node-10003"]);
        assert_eq!(failure_detector.garbage_collect(), Vec::new());

        // Wait for dead_node_grace_period & garbage collect.
        tokio::time::advance(Duration::from_secs(25 * 60 * 60)).await;
        let garbage_collected_nodes = failure_detector.garbage_collect();
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
        assert_eq!(
            failure_detector
                .dead_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
        let mut removed_nodes = garbage_collected_nodes
            .iter()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        removed_nodes.sort_unstable();
        assert_eq!(
            removed_nodes,
            vec!["node-10001", "node-10002", "node-10003"]
        );
    }

    #[tokio::test]
    async fn test_failure_detector_node_state_from_live_to_down_to_live() {
        tokio::time::pause();
        let mut rng = rand::thread_rng();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let intervals_choices = [1u64, 2];
        let node_1 = ChitchatId::for_local_test(10_001);

        for _ in 0..=2000 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            tokio::time::advance(Duration::from_secs(*time_offset)).await;
            failure_detector.report_heartbeat(&node_1);
        }

        failure_detector.update_node_liveness(&node_1);
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-10001"]
        );

        // Check node-1 is down (stop reporting heartbeat).
        tokio::time::advance(Duration::from_secs(20)).await;
        failure_detector.update_node_liveness(&node_1);
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );

        // Check node-1 is back up (resume reporting heartbeat).
        for _ in 0..=500 {
            let time_offset = intervals_choices.choose(&mut rng).unwrap();
            tokio::time::advance(Duration::from_secs(*time_offset)).await;
            failure_detector.report_heartbeat(&node_1);
        }
        failure_detector.update_node_liveness(&node_1);
        assert_eq!(
            failure_detector
                .live_nodes()
                .map(|chitchat_id| chitchat_id.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-10001"]
        );
    }

    #[tokio::test]
    async fn test_failure_detector_node_state_additive_smoothing_predominant_in_the_beginning() {
        tokio::time::pause();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());

        // We add a few very short samples.
        let chitchat_id = ChitchatId::for_local_test(10_001);
        failure_detector.report_heartbeat(&chitchat_id);

        tokio::time::advance(Duration::from_secs(1)).await;
        let chitchat_id = ChitchatId::for_local_test(10_001);
        for _ in 0..5 {
            tokio::time::advance(Duration::from_millis(200)).await;
            failure_detector.report_heartbeat(&chitchat_id);
        }

        tokio::time::advance(Duration::from_secs(6)).await;
        failure_detector.update_node_liveness(&chitchat_id);

        let live_nodes = failure_detector
            .live_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(live_nodes, vec!["node-10001"]);

        tokio::time::advance(Duration::from_secs(40)).await;
        failure_detector.update_node_liveness(&chitchat_id);

        let live_nodes = failure_detector
            .live_nodes()
            .map(|chitchat_id| chitchat_id.node_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(live_nodes, Vec::<&str>::new());
    }

    #[tokio::test]
    async fn test_failure_detector_node_state_additive_smoothing_effect_fades_off() {
        tokio::time::pause();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());

        // We add a few very short samples.
        let chitchat_id = ChitchatId::for_local_test(10_001);
        failure_detector.report_heartbeat(&chitchat_id);

        let chitchat_id = ChitchatId::for_local_test(10_001);
        for _ in 0..1000 {
            tokio::time::advance(Duration::from_millis(200)).await;
            failure_detector.report_heartbeat(&chitchat_id);
        }

        tokio::time::advance(Duration::from_secs(6)).await;

        failure_detector.update_node_liveness(&chitchat_id);

        assert!(failure_detector.live_nodes().next().is_none());
    }

    #[tokio::test]
    async fn test_sampling_window() {
        tokio::time::pause();
        let mut sampling_window =
            SamplingWindow::new(10, Duration::from_secs(5), Duration::from_secs(2));
        sampling_window.report_heartbeat();

        tokio::time::advance(Duration::from_secs(3)).await;
        sampling_window.report_heartbeat();

        // Now intervals window is: [3.0].
        let mean = (3.0 + 2.0 * 5.0) / (1.0f64 + 5.0f64);

        // 0s elapsed since last reported heartbeat.
        assert_nearly_equal(sampling_window.phi().unwrap(), 0.0f64);

        // 1s elapsed since last reported heartbeat.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_nearly_equal(sampling_window.phi().unwrap(), 1.0f64 / mean);

        // Check reported heartbeat later than max_interval is ignore.
        tokio::time::advance(Duration::from_secs(5)).await;
        sampling_window.report_heartbeat();
        tokio::time::advance(Duration::from_secs(2)).await;

        assert_nearly_equal(sampling_window.phi().unwrap(), 2.0f64 / mean);

        tokio::time::advance(Duration::from_secs(100)).await;
        sampling_window.reset();

        // To revive, a single sample is not sufficient.
        sampling_window.report_heartbeat();
        assert!(sampling_window.phi().is_none());

        tokio::time::advance(Duration::from_secs(2)).await;
        sampling_window.report_heartbeat();

        tokio::time::advance(Duration::from_secs(4)).await;

        // Now intervals window is: [2.0]. With additive smoothing we get:
        let new_mean = (2.0 + 2.0 * 5.0) / (1.0f64 + 5.0f64);

        assert_nearly_equal(sampling_window.phi().unwrap(), 4.0f64 / new_mean);
    }

    #[track_caller]
    fn assert_nearly_equal(value: f64, expected: f64) {
        assert!(
            (value - expected).abs() < f64::EPSILON,
            "value ({value}) is not not nearly equal to expected {expected}"
        );
    }

    #[test]
    fn test_bounded_array_stats() {
        let capacity = 5;
        let mut bounded_array = BoundedArrayStats::with_capacity(capacity);
        let mut queue: VecDeque<f64> = VecDeque::new();
        for i in 1..=capacity {
            assert!(bounded_array.len() < capacity);
            assert!(!bounded_array.is_filled);
            bounded_array.append(i as f64);
            queue.push_back(i as f64);
            assert_eq!(bounded_array.len(), i);
            assert_eq!(queue.len(), i);
            assert_nearly_equal(bounded_array.sum(), queue.iter().copied().sum::<f64>());
        }

        assert!(bounded_array.is_filled);
        assert_nearly_equal(bounded_array.sum(), queue.iter().copied().sum());

        for i in capacity + 1..capacity * 2 {
            bounded_array.append(i as f64);
            queue.push_back(i as f64);
            queue.pop_front();
            assert_nearly_equal(bounded_array.sum(), queue.iter().copied().sum::<f64>());
            assert_eq!(queue.len(), capacity);
            assert_eq!(bounded_array.len(), capacity);
        }
    }
}
