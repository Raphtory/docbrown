use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use rustc_hash::FxHasher;

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn calculate_fast_hash<T: Hash>(t: &T) -> u64 {
    let mut s = FxHasher::default();
    t.hash(&mut s);
    s.finish()
}

pub fn get_shard_id_from_global_vid<K: std::hash::Hash>(v_id: K, n_shards: usize) -> usize {
    let v: usize = calculate_fast_hash(&v_id).try_into().unwrap();
    v % n_shards
}
