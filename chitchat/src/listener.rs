use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

/// Event triggered when a key is added, updated, or removed.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyEvent {
    /// A key was added.
    Added { key: String, value: String },
    /// The value of a key was updated.
    Updated {
        key: String,
        previous_value: String,
        new_value: String,
    },
    /// A key was removed.
    Removed { key: String, value: String },
}

impl KeyEvent {
    pub fn key(&self) -> &str {
        match self {
            KeyEvent::Added { key, .. } => key,
            KeyEvent::Updated { key, .. } => key,
            KeyEvent::Removed { key, .. } => key,
        }
    }
}

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

type BoxedListener = Box<dyn Fn(&str, &KeyEvent) + Send + Sync + 'static>;

#[derive(Default, Clone)]
pub(crate) struct Listeners {
    inner: Arc<RwLock<InnerListeners>>,
}

impl Listeners {
    #[must_use]
    pub(crate) fn subscribe_event(
        &self,
        key_prefix: impl ToString,
        callback: impl Fn(&str, &KeyEvent) + Send + Sync + 'static,
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
        let weak_listeners = Arc::downgrade(&self.inner);
        let mut inner_listener_guard = self.inner.write().unwrap();
        let listener_id = inner_listener_guard
            .listener_idx
            .fetch_add(1, Ordering::Relaxed);
        inner_listener_guard.subscribe_event(&key_prefix, listener_id, boxed_listener);
        ListenerHandle {
            prefix: key_prefix,
            listener_id,
            listeners: weak_listeners,
        }
    }

    pub(crate) fn trigger_event(&mut self, node_id: &str, event: KeyEvent) {
        self.inner.read().unwrap().trigger_event(node_id, event);
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

    fn trigger_event(&self, node_id: &str, event: KeyEvent) {
        // We treat the empty prefix a tiny bit separately to get able to at least
        // use the first character as a range bound, as if we were going to the first level of
        // a trie.
        if let Some(listeners) = self.listeners.get("") {
            for listener in listeners.values() {
                (*listener)(node_id, &event);
            }
        }
        let key = event.key();

        if key.is_empty() {
            return;
        }
        let range = (Bound::Included(&key[0..1]), Bound::Included(key));

        for (prefix_key, listeners) in self.listeners.range::<str, _>(range) {
            if prefix_key.as_str() > key {
                break;
            }
            for listener in listeners.values() {
                (*listener)(node_id, &event);
            }
        }
    }

    fn remove_listener(&mut self, key_prefix: &str, listener_id: usize) {
        if let Some(callbacks) = self.listeners.get_mut(key_prefix) {
            callbacks.remove(&listener_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listeners_simple() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe_event("prefix:", move |node_id, event| {
            assert_eq!(node_id, "test-node");

            let KeyEvent::Added { key, value } = event else {
                panic!("expected `KeyEvent::Added` variant, got `{event:?}`");
            };
            assert_eq!(key, "prefix:key");
            assert_eq!(value, "value");

            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "prefix:key".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event.clone());
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        std::mem::drop(handle);
        listeners.trigger_event("test-node", event);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_listeners_empty_prefix() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        listeners
            .subscribe_event("", move |node_id, event| {
                assert_eq!(node_id, "test-node");

                let KeyEvent::Added { key, value } = event else {
                    panic!("expected `Key::Added` variant, got `{event:?}`");
                };
                assert_eq!(key, "prefix:key");
                assert_eq!(value, "value");

                counter_clone.fetch_add(1, Ordering::Relaxed);
            })
            .forever();
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "prefix:key".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn test_listeners_forever() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe_event("", move |node_id, event| {
            assert_eq!(node_id, "test-node");

            let KeyEvent::Added { key, value } = event else {
                panic!("expected `Key::Added` variant, got `{event:?}`");
            };
            assert_eq!(key, "prefix:key");
            assert_eq!(value, "value");

            counter_clone.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "prefix:key".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event.clone());
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        handle.forever();
        listeners.trigger_event("test-node", event);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_listeners_prefixes() {
        let mut listeners = Listeners::default();

        let subscribe_event = |prefix: &str| {
            let counter: Arc<AtomicUsize> = Default::default();
            let counter_clone = counter.clone();
            listeners
                .subscribe_event(prefix, move |_key, _value| {
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

        let event = KeyEvent::Added {
            key: "hello".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter_empty.load(Ordering::Relaxed), 1);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter_empty.load(Ordering::Relaxed), 2);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "a".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter_empty.load(Ordering::Relaxed), 3);
        assert_eq!(counter_b.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "b".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter_empty.load(Ordering::Relaxed), 4);
        assert_eq!(counter_b.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "ba".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter_empty.load(Ordering::Relaxed), 5);
        assert_eq!(counter_b.load(Ordering::Relaxed), 2);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 0);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);

        let event = KeyEvent::Added {
            key: "bb".to_string(),
            value: "value".to_string(),
        };
        listeners.trigger_event("test-node", event);
        assert_eq!(counter_empty.load(Ordering::Relaxed), 6);
        assert_eq!(counter_b.load(Ordering::Relaxed), 3);
        assert_eq!(counter_bb.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bb2.load(Ordering::Relaxed), 1);
        assert_eq!(counter_bc.load(Ordering::Relaxed), 0);
    }
}
