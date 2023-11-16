use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};

pub struct ListenerHandle {
    prefix: String,
    idx: usize,
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
            listeners_guard.remove_listener(&self.prefix, self.idx);
        }
    }
}

type BoxedListener = Box<dyn Fn(&str, &str) + 'static + Send + Sync>;

#[derive(Default, Clone)]
pub(crate) struct Listeners {
    inner: Arc<RwLock<InnerListeners>>,
}

impl Listeners {
    #[must_use]
    pub(crate) fn subscribe_event(
        &self,
        key_prefix: impl ToString,
        callback: impl Fn(&str, &str) + 'static + Send + Sync,
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
            .fetch_add(1, Ordering::SeqCst);
        inner_listener_guard.subscribe_event(&key_prefix, new_idx, boxed_listener);
        ListenerHandle {
            prefix: key_prefix,
            idx: new_idx,
            listeners: weak_listeners,
        }
    }

    pub(crate) fn trigger_event(&mut self, key: &str, value: &str) {
        self.inner.read().unwrap().trigger_event(key, value);
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

    fn trigger_event(&self, key: &str, value: &str) {
        // We treat the empty prefix a tiny bit separately to get able to at least
        // use the first character as a range bound, as if we were going to the first level of
        // a trie.
        if let Some(listeners) = self.listeners.get("") {
            for listener in listeners.values() {
                (*listener)(key, value);
            }
        }
        if key.is_empty() {
            return;
        }

        let range = (Bound::Included(&key[0..1]), Bound::Included(key));
        for (prefix_key, listeners) in self.listeners.range::<str, _>(range) {
            if prefix_key.as_str() > key {
                break;
            }
            if let Some(stripped_key) = key.strip_prefix(prefix_key) {
                for listener in listeners.values() {
                    (*listener)(stripped_key, value);
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

    #[test]
    fn test_listeners_simple() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe_event("prefix:", move |key, value| {
            assert_eq!(key, "strippedprefix");
            assert_eq!(value, "value");
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        listeners.trigger_event("prefix:strippedprefix", "value");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        std::mem::drop(handle);
        listeners.trigger_event("prefix:strippedprefix", "value");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_listeners_empty_prefix() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        listeners
            .subscribe_event("", move |key, value| {
                assert_eq!(key, "prefix:strippedprefix");
                assert_eq!(value, "value");
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .forever();
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        listeners.trigger_event("prefix:strippedprefix", "value");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn test_listeners_forever() {
        let mut listeners = Listeners::default();
        let counter: Arc<AtomicUsize> = Default::default();
        let counter_clone = counter.clone();
        let handle = listeners.subscribe_event("prefix:", move |key, value| {
            assert_eq!(key, "strippedprefix");
            assert_eq!(value, "value");
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        listeners.trigger_event("prefix:strippedprefix", "value");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        handle.forever();
        listeners.trigger_event("prefix:strippedprefix", "value");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_listeners_prefixes() {
        let mut listeners = Listeners::default();

        let subscribe_event = |prefix: &str| {
            let counter: Arc<AtomicUsize> = Default::default();
            let counter_clone = counter.clone();
            listeners
                .subscribe_event(prefix, move |key, value| {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                })
                .forever();
            counter
        };

        let counter_empty = subscribe_event("");
        let counter_b = subscribe_event("b");
        let counter_bb = subscribe_event("bb");
        let counter_bb2 = subscribe_event("bb");
        let counter_bc = subscribe_event("bc");

        listeners.trigger_event("hello", "value");
        assert_eq!(counter_empty.load(Ordering::SeqCst), 1);
        assert_eq!(counter_b.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb2.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bc.load(Ordering::SeqCst), 0);

        listeners.trigger_event("", "value");
        assert_eq!(counter_empty.load(Ordering::SeqCst), 2);
        assert_eq!(counter_b.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb2.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bc.load(Ordering::SeqCst), 0);

        listeners.trigger_event("a", "value");
        assert_eq!(counter_empty.load(Ordering::SeqCst), 3);
        assert_eq!(counter_b.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb2.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bc.load(Ordering::SeqCst), 0);

        listeners.trigger_event("b", "value");
        assert_eq!(counter_empty.load(Ordering::SeqCst), 4);
        assert_eq!(counter_b.load(Ordering::SeqCst), 1);
        assert_eq!(counter_bb.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb2.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bc.load(Ordering::SeqCst), 0);

        listeners.trigger_event("ba", "value");
        assert_eq!(counter_empty.load(Ordering::SeqCst), 5);
        assert_eq!(counter_b.load(Ordering::SeqCst), 2);
        assert_eq!(counter_bb.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bb2.load(Ordering::SeqCst), 0);
        assert_eq!(counter_bc.load(Ordering::SeqCst), 0);

        listeners.trigger_event("bb", "value");
        assert_eq!(counter_empty.load(Ordering::SeqCst), 6);
        assert_eq!(counter_b.load(Ordering::SeqCst), 3);
        assert_eq!(counter_bb.load(Ordering::SeqCst), 1);
        assert_eq!(counter_bb2.load(Ordering::SeqCst), 1);
        assert_eq!(counter_bc.load(Ordering::SeqCst), 0);
    }
}
