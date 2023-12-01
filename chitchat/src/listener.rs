use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

use crate::KeyChangeEvent;

pub struct ListenerHandle {
    prefix: String,
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
            listeners_guard.remove_listener(&self.prefix, self.listener_id);
        }
    }
}

type BoxedListener = Box<dyn Fn(KeyChangeEvent) + 'static + Send + Sync>;

#[derive(Default, Clone)]
pub(crate) struct Listeners {
    inner: Arc<RwLock<InnerListeners>>,
}

impl Listeners {
    #[must_use]
    pub(crate) fn subscribe_event(
        &self,
        key_prefix: impl ToString,
        callback: impl Fn(KeyChangeEvent) + 'static + Send + Sync,
    ) -> ListenerHandle {
        let key_prefix = key_prefix.to_string();
        let boxed_listener = Box::new(callback);
        self.subscribe_event_for_ligher_monomorphization(key_prefix, boxed_listener)
    }

    fn subscribe_event_for_ligher_monomorphization(
        &self,
        key_prefix: String,
        boxed_listener: BoxedListener,
    ) -> ListenerHandle {
        let key_prefix = key_prefix.to_string();
        let weak_listeners = Arc::downgrade(&self.inner);
        let mut inner_listener_guard = self.inner.write().unwrap();
        let new_idx = inner_listener_guard
            .listener_idx
            .fetch_add(1, Ordering::Relaxed);
        inner_listener_guard.subscribe_event(&key_prefix, new_idx, boxed_listener);
        ListenerHandle {
            prefix: key_prefix,
            listener_id: new_idx,
            listeners: weak_listeners,
        }
    }

    pub(crate) fn trigger_event(&mut self, key_change_event: KeyChangeEvent) {
        self.inner.read().unwrap().trigger_event(key_change_event);
    }
}

#[derive(Default)]
struct InnerListeners {
    // A trie would have been more efficient, but in reality we don't have
    // that many listeners.
    listeners: BTreeMap<String, HashMap<usize, BoxedListener>>,
    listener_idx: AtomicUsize,
}

impl InnerListeners {
    // We don't inline this to make sure monomorphization generates as little code as possible.
    fn subscribe_event(&mut self, key_prefix: &str, idx: usize, callback: BoxedListener) {
        if let Some(callbacks) = self.listeners.get_mut(key_prefix) {
            callbacks.insert(idx, callback);
        } else {
            let mut listener_map = HashMap::new();
            listener_map.insert(idx, callback);
            self.listeners.insert(key_prefix.to_string(), listener_map);
        }
    }

    fn trigger_event(&self, key_change_event: KeyChangeEvent) {
        // We treat the empty prefix a tiny bit separately to get able to at least
        // use the first character as a range bound, as if we were going to the first level of
        // a trie.
        if let Some(listeners) = self.listeners.get("") {
            for listener in listeners.values() {
                (*listener)(key_change_event);
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
                for listener in listeners.values() {
                    (*listener)(stripped_key_change_event);
                }
            }
        }
    }

    fn remove_listener(&mut self, key_prefix: &str, idx: usize) {
        if let Some(callbacks) = self.listeners.get_mut(key_prefix) {
            callbacks.remove(&idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChitchatId;

    fn chitchat_id(port: u16) -> ChitchatId {
        ChitchatId::new(format!("node{port}"), 0, ([127, 0, 0, 1], port).into())
    }

    #[test]
    fn test_listeners_simple() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe_event("prefix:", move |key_change_event| {
            assert_eq!(key_change_event.key, "strippedprefix");
            assert_eq!(key_change_event.value, "value");
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        let node_id = chitchat_id(7280u16);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        listeners.trigger_event(KeyChangeEvent {
            key: "prefix:strippedprefix",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        std::mem::drop(handle);
        let node_id = chitchat_id(7280u16);
        listeners.trigger_event(KeyChangeEvent {
            key: "prefix:strippedprefix",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_listeners_empty_prefix() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        listeners
            .subscribe_event("", move |key_change_event| {
                assert_eq!(key_change_event.key, "prefix:strippedprefix");
                assert_eq!(key_change_event.value, "value");
                counter_clone.fetch_add(1, Ordering::Relaxed);
            })
            .forever();
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        let node_id = chitchat_id(7280u16);
        let key_change_event = KeyChangeEvent {
            key: "prefix:strippedprefix",
            value: "value",
            node: &node_id,
        };
        listeners.trigger_event(key_change_event);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn test_listeners_forever() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe_event("prefix:", move |evt| {
            assert_eq!(evt.key, "strippedprefix");
            assert_eq!(evt.value, "value");
            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        let node_id = chitchat_id(7280u16);
        listeners.trigger_event(KeyChangeEvent {
            key: "prefix:strippedprefix",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        handle.forever();
        listeners.trigger_event(KeyChangeEvent {
            key: "prefix:strippedprefix",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_listeners_prefixes() {
        let mut listeners = Listeners::default();

        let subscribe_event = |prefix: &str| {
            let counter: Arc<AtomicUsize> = Default::default();
            let counter_clone = counter.clone();
            listeners
                .subscribe_event(prefix, move |_evt| {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
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
        listeners.trigger_event(KeyChangeEvent {
            key: "hello",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 1);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);
        listeners.trigger_event(KeyChangeEvent {
            key: "",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 2);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_event(KeyChangeEvent {
            key: "a",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 3);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_event(KeyChangeEvent {
            key: "b",
            value: "value",
            node: &node_id,
        });

        assert_eq!(counter_empty.load(Ordering::Relaxed), 4);
        assert_eq!(counter_b.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_event(KeyChangeEvent {
            key: "ba",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 5);
        assert_eq!(counter_b.load(Ordering::Relaxed), 2);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        listeners.trigger_event(KeyChangeEvent {
            key: "bb",
            value: "value",
            node: &node_id,
        });
        assert_eq!(counter_empty.load(Ordering::Relaxed), 6);
        assert_eq!(counter_b.load(Ordering::Relaxed), 3);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);
    }
}
