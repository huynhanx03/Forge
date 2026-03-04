use std::mem::replace;

#[derive(Debug, Clone, PartialEq)]
pub struct FlatMap<K, V> {
    data: Vec<(K, V)>,
}

impl<K: Ord, V> FlatMap<K, V> {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.data.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(idx) => Some(replace(&mut self.data[idx].1, value)),
            Err(idx) => {
                self.data.insert(idx, (key, value));
                None
            }
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match self.data.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(idx) => Some(&mut self.data[idx].1),
            Err(_) => None,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self.data.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(idx) => Some(&self.data[idx].1),
            Err(_) => None,
        }
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.data.iter().map(|(_, v)| v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FlatSet<T> {
    data: Vec<T>,
}

impl<T: Ord> FlatSet<T> {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn insert(&mut self, value: T) -> bool {
        match self.data.binary_search(&value) {
            Ok(_) => false,
            Err(idx) => {
                self.data.insert(idx, value);
                true
            }
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
