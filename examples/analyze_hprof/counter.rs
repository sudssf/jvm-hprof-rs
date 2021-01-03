use nom::lib::std::ops::AddAssign;
use std::{borrow, collections, hash};

/// A convenience wrapper around a HashMap<K, u64>.
pub(crate) struct Counter<K, V: CounterValue = u64> {
    counts: collections::HashMap<K, V>,
}

impl<K: Eq + hash::Hash, V: CounterValue> Counter<K, V> {
    pub(crate) fn new() -> Counter<K, V> {
        Counter {
            counts: collections::HashMap::new(),
        }
    }

    pub(crate) fn increment(&mut self, key: K) {
        self.counts
            .entry(key)
            .and_modify(|count| *count += V::one())
            .or_insert(V::one());
    }

    #[allow(unused)] // used in tests, and it feels weird to only allow iteration?
    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: borrow::Borrow<Q>,
        Q: hash::Hash + Eq + ?Sized,
    {
        self.counts.get(key).map(|&r| r)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.counts.iter()
    }

    pub(crate) fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.counts.retain(f)
    }
}

impl<K: Eq + hash::Hash> Default for Counter<K> {
    fn default() -> Self {
        Counter::new()
    }
}

impl<K: Eq + hash::Hash> std::ops::AddAssign for Counter<K> {
    fn add_assign(&mut self, rhs: Self) {
        rhs.counts.into_iter().for_each(|(key, count)| {
            self.counts
                .entry(key)
                .and_modify(|orig_count| *orig_count += count)
                .or_insert(count);
        })
    }
}

pub(crate) trait CounterValue: AddAssign + Sized + Copy {
    fn one() -> Self;
}

impl CounterValue for u64 {
    fn one() -> Self {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn get_gets() {
        let mut counter = Counter::new();

        counter.increment("foo");
        counter.increment("foo");
        counter.increment("bar");

        assert_eq!(Some(2), counter.get("foo"));
        assert_eq!(Some(1), counter.get("bar"));
        assert_eq!(None, counter.get("baz"));
    }

    #[test]
    fn add_assign_sums() {
        let mut counter = Counter::new();

        counter.increment("foo");
        counter.increment("foo");
        counter.increment("bar");

        let mut counter2 = Counter::new();

        counter2.increment("foo");
        counter2.increment("quux");

        counter += counter2;

        let mut pairs = counter.iter().map(|(&k, &v)| (k, v)).collect_vec();
        pairs.sort_by_key(|&(key, _val)| key);

        assert_eq!(vec![("bar", 1_u64), ("foo", 3), ("quux", 1)], pairs);
    }
}
