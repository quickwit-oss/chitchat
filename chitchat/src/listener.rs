use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Arc, RwLock, Weak};

use tracing::error;

use crate::{KeyChangeEvent, KeyChangeEventRef};

pub struct ListenerHandle {
    // The prefix and listener_id are used for removal of the listener.
    listener_id: usize,
    listeners: Weak<RwLock<InnerListeners>>,
}

impl ListenerHandle {
    // By default, a listener is cancelled when its handle is dropped.
    // Calling forever prevents that.
    //
    // The listener itself will only be dropped when the Listeners object is dropped.
    pub fn forever(mut self) {
        self.listeners = Weak::new();
    }
}

impl Drop for ListenerHandle {
    fn drop(&mut self) {
        if let Some(listeners) = self.listeners.upgrade() {
            let mut listeners_guard = listeners.write().unwrap();
            listeners_guard.remove_listener(self.listener_id);
        }
    }
}

type BoxedListener = Box<dyn Fn(&[KeyChangeEventRef]) + 'static + Send + Sync>;

#[derive(Default)]
pub(crate) struct Listeners {
    inner: Arc<RwLock<InnerListeners>>,
}

impl Listeners {
    #[must_use]
    pub(crate) fn subscribe(
        &self,
        key_prefix: impl ToString,
        callback: impl Fn(&[KeyChangeEventRef]) + 'static + Send + Sync,
    ) -> ListenerHandle {
        let key_prefix = key_prefix.to_string();
        let boxed_listener = Box::new(callback);
        self.subscribe_events_for_ligher_monomorphization(key_prefix, boxed_listener)
    }

    fn subscribe_events_for_ligher_monomorphization(
        &self,
        key_prefix: String,
        boxed_listener: BoxedListener,
    ) -> ListenerHandle {
        let key_prefix = key_prefix.to_string();
        let weak_listeners = Arc::downgrade(&self.inner);
        let mut inner_listener_guard = self.inner.write().unwrap();
        let new_idx = inner_listener_guard.listener_idx;
        inner_listener_guard.listener_idx += 1;
        let callback_entry = CallbackEntry {
            prefix: key_prefix.clone(),
            callback: boxed_listener,
        };
        inner_listener_guard
            .callbacks
            .insert(new_idx, callback_entry);
        inner_listener_guard.subscribe_events(&key_prefix, new_idx);
        ListenerHandle {
            listener_id: new_idx,
            listeners: weak_listeners,
        }
    }

    #[cfg(test)]
    pub(crate) fn trigger_single_event(&self, key_change_event: KeyChangeEvent) {
        self.inner
            .read()
            .unwrap()
            .trigger_events(&[key_change_event]);
    }

    pub(crate) fn trigger_events(&self, key_change_events: &[KeyChangeEvent]) {
        self.inner.read().unwrap().trigger_events(key_change_events);
    }
}

struct CallbackEntry {
    prefix: String,
    callback: BoxedListener,
}

type CallbackId = usize;

#[derive(Default)]
struct InnerListeners {
    // A trie would have been more efficient, but in reality we don't have
    // that many listeners.
    listeners: BTreeMap<String, Vec<CallbackId>>,
    listener_idx: usize,
    // Callbacks is a hashmap because as we delete listeners, we create "holes" in the
    // callback_id -> callback mapping
    callbacks: HashMap<usize, CallbackEntry>,
}

impl InnerListeners {
    // We don't inline this to make sure monomorphization generates as little code as possible.
    fn subscribe_events(&mut self, key_prefix: &str, idx: CallbackId) {
        self.listeners
            .entry(key_prefix.to_string())
            .or_default()
            .push(idx);
    }

    fn call(&self, callback_id: CallbackId, key_change_event: &[KeyChangeEventRef]) {
        let Some(CallbackEntry { callback, .. }) = self.callbacks.get(&callback_id) else {
            error!(
                "callback {callback_id} not found upon call. this should not happen, please report"
            );
            return;
        };
        (*callback)(key_change_event);
    }

    fn collect_events_to_trigger<'a>(
        &self,
        key_change_event: KeyChangeEventRef<'a>,
        callback_to_events: &mut HashMap<CallbackId, Vec<KeyChangeEventRef<'a>>>,
    ) {
        // We treat the empty prefix a tiny bit separately to get able to at least
        // use the first character as a range bound, as if we were going to the first level of
        // a trie.
        if let Some(callback_ids) = self.listeners.get("") {
            for &callback_id in callback_ids {
                callback_to_events
                    .entry(callback_id)
                    .or_default()
                    .push(key_change_event);
            }
        }
        if key_change_event.key.is_empty() {
            return;
        }

        let range = (
            Bound::Included(&key_change_event.key[0..1]),
            Bound::Included(key_change_event.key),
        );
        for (prefix_key, listeners) in self.listeners.range::<str, _>(range) {
            if prefix_key.as_str() > key_change_event.key {
                break;
            }
            if let Some(stripped_key_change_event) = key_change_event.strip_key_prefix(prefix_key) {
                for &callback_id in listeners {
                    callback_to_events
                        .entry(callback_id)
                        .or_default()
                        .push(stripped_key_change_event);
                }
            }
        }
    }

    fn trigger_events(&self, key_change_events: &[KeyChangeEvent]) {
        let mut callback_to_events: HashMap<CallbackId, Vec<KeyChangeEventRef>> =
            HashMap::default();
        // We aggregate events to trigger per callback, so that we can call each callback
        // with a batch of events.
        for key_change_event in key_change_events {
            self.collect_events_to_trigger(key_change_event.into(), &mut callback_to_events);
        }
        for (callback_id, key_change_events) in callback_to_events {
            self.call(callback_id, &key_change_events[..]);
        }
    }

    fn remove_listener(&mut self, callback_id: CallbackId) {
        let Some(CallbackEntry { prefix, .. }) = self.callbacks.remove(&callback_id) else {
            error!(
                "callback {callback_id} not found upon remove. this should not happen, please \
                 report"
            );
            return;
        };
        let Some(callbacks) = self.listeners.get_mut(&prefix) else {
            error!(
                "callback prefix not foudn upon remove. this should never happen, please report"
            );
            return;
        };
        let position = callbacks
            .iter()
            .position(|x| *x == callback_id)
            .expect("callback not found");
        callbacks.swap_remove(position);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::ChitchatId;

    fn chitchat_id(port: u16) -> ChitchatId {
        ChitchatId::new(format!("node{port}"), 0, ([127, 0, 0, 1], port).into())
    }

    #[test]
    fn test_listeners_simple() {
        let listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe("prefix:", move |events| {
            assert_eq!(events.len(), 1);
            let key_change_event = events[0];
            assert_eq!(key_change_event.key, "strippedprefix");
            assert_eq!(key_change_event.value, "value");
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        let node_id = chitchat_id(7280u16);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        listeners.trigger_single_event(KeyChangeEvent {
            key: "prefix:strippedprefix".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        std::mem::drop(handle);
        let node_id = chitchat_id(7280u16);
        listeners.trigger_single_event(KeyChangeEvent {
            key: "prefix:strippedprefix".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_listeners_empty_prefix() {
        let listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        listeners
            .subscribe("", move |events| {
                assert_eq!(events.len(), 1);
                let key_change_event = &events[0];
                assert_eq!(key_change_event.key, "prefix:strippedprefix");
                assert_eq!(key_change_event.value, "value");
                counter_clone.fetch_add(1, Ordering::Relaxed);
            })
            .forever();
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        let node_id = chitchat_id(7280u16);
        let key_change_event = KeyChangeEvent {
            key: "prefix:strippedprefix".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        };
        listeners.trigger_single_event(key_change_event);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_listeners_forever() {
        let listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe("prefix:", move |evts| {
            assert_eq!(evts.len(), 1);
            let evt = evts[0];
            assert_eq!(evt.key, "strippedprefix");
            assert_eq!(evt.value, "value");
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        let node_id = chitchat_id(7280u16);
        listeners.trigger_single_event(KeyChangeEvent {
            key: "prefix:strippedprefix".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        handle.forever();
        listeners.trigger_single_event(KeyChangeEvent {
            key: "prefix:strippedprefix".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_listeners_prefixes() {
        let listeners = Listeners::default();

        let subscribe_event = |prefix: &str| {
            let counter: Arc<AtomicUsize> = Default::default();
            let counter_clone = counter.clone();
            listeners
                .subscribe(prefix, move |events| {
                    assert_eq!(events.len(), 1);
                    counter_clone.fetch_add(events.len(), Ordering::Relaxed);
                })
                .forever();
            counter
        };

        let counter_empty = subscribe_event("");
        let counter_b = subscribe_event("b");
        let counter_bb = subscribe_event("bb");
        let counter_bb2 = subscribe_event("bb");
        let counter_bc = subscribe_event("bc");

        let node_id = chitchat_id(7280u16);
        listeners.trigger_single_event(KeyChangeEvent {
            key: "hello".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 1);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);
        listeners.trigger_single_event(KeyChangeEvent {
            key: "".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 2);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_single_event(KeyChangeEvent {
            key: "a".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 3);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_single_event(KeyChangeEvent {
            key: "b".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });

        assert_eq!(counter_empty.load(Ordering::Relaxed), 4);
        assert_eq!(counter_b.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_single_event(KeyChangeEvent {
            key: "ba".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 5);
        assert_eq!(counter_b.load(Ordering::Relaxed), 2);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_single_event(KeyChangeEvent {
            key: "bb".to_string(),
            value: "value".to_string(),
            node: node_id.clone(),
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 6);
        assert_eq!(counter_b.load(Ordering::Relaxed), 3);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);
    }
}
