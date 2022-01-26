use std::{time::{Instant, Duration}, collections::{HashMap, BTreeMap}, sync::Mutex, };


use crate::delta::Delta;

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


pub struct FailureDetector {
    node_samples: Mutex<BTreeMap<String, SamplingWindow>>,
    window_size: usize,
    max_interval: Duration,
    initial_interval: Duration,
}

impl FailureDetector {

    pub fn new(window_size: usize, max_interval: Duration, initial_interval: Duration) -> Self {
        Self {
            node_samples: Mutex::new(BTreeMap::new()),
            window_size,
            max_interval,
            initial_interval,
        }
    }

    // report endpoint heartbeat
    pub fn report_heartbeat(&mut self, node_id: &str) {
        let mut node_samples = self.node_samples.lock().unwrap();
        let heartbeat_window = node_samples.entry(node_id.to_string())
            .or_insert_with(|| SamplingWindow::new(self.window_size, self.max_interval, self.initial_interval));
            heartbeat_window.append();


        //logic to add
        println!("heartbeat for node -> {}", node_id);
    }

    pub fn remove(&mut self, node_id: &str) {
        self.node_samples.lock().unwrap().remove(node_id);
    }

    pub fn phi(&mut self, node_id: &str) -> f64 {
        self.node_samples.lock().unwrap().get(node_id).unwrap().phi()
    }

}









struct SamplingWindow {
    intervals: BoundedArrayStats,
    last_value: Option<Instant>,
    // Drop heartbeats longer than this (fd configured) 
    max_interval: Duration,
    // the initial interval on startup (fd configured) 
    initial_interval: Duration,
}

impl SamplingWindow {

    pub fn new (window_size: usize, max_interval: Duration, initial_interval: Duration) -> Self {
        Self {
            intervals: BoundedArrayStats::new(window_size),
            last_value: None,
            max_interval,
            initial_interval,
        }
    }

    pub fn append(&mut self) {
        if let Some(last_value) =  &self.last_value {
            let interval = last_value.elapsed();
            if interval <= self.max_interval {
                self.intervals.append(interval.as_secs_f64());
            }
        } else {
            self.intervals.append(self.initial_interval.as_secs_f64());
        };
        self.last_value = Some(Instant::now());
    }

    pub fn phi(&self) -> f64 {
        // ensure we don't call before any sample arrival.
        assert!(self.intervals.mean() > 0.0 && self.last_value.is_some());

        let interval = self.last_value.unwrap().elapsed().as_secs_f64();
        interval / self.intervals.mean()
    }

}


struct BoundedArrayStats {
    data: Vec<f64>,
    size: usize,
    is_filled: bool,
    index: usize,
    sum: f64,
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

    pub fn mean(&self) -> f64 {
        self.mean
    }

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
        
        self.mean =  self.sum / self.len() as f64;
    }

    fn len(&self) -> usize {
        if self.is_filled {
            return self.index;
        }
        self.size
    }

}
