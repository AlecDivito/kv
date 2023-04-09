use bit_vec::BitVec;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hash, Hasher};

/// A BloomFilter is a space effeint way to store the likely hood a given value
/// is contained inside of a set. A Bloom filter is good for telling you if a
/// value is **not** in a set but not great at telling you if a value is in a
/// set.
///
/// This implmentation is from a blog post that I read here:
/// https://richardstartin.github.io/posts/building-a-bloom-filter-from-scratch
///
/// A fast standard Bloom Filter implementation that requires only two
/// hash functions, generated by `std::collections::hash_map::DefaultHasher`.
///
/// If an item is not present in the filter then `contains` is guaranteed
/// to return `false` for the queried item.
///
/// The probability that `contains` returns `true` for an item that is not
/// present in the filter is called the False Positive Rate.
pub struct BloomFilter {
    bitmap: BitVec,
    /// Size of the bit array.
    optimal_m: usize,
    /// Number of hash functions.
    optimal_k: u32,
    /// Two hash functions from which k number of hashes are derived.
    hashers: [DefaultHasher; 2],
}

impl BloomFilter {
    /// Create a new StandardBloomFilter that expects to store `items_count`
    /// membership with a false positive rate of the value specified in `fp_rate`.
    pub fn new(items_count: usize, fp_rate: f64) -> Self {
        let optimal_m = Self::bitmap_size(items_count, fp_rate);
        let optimal_k = Self::optimal_k(fp_rate);
        let hashers = [
            RandomState::new().build_hasher(),
            RandomState::new().build_hasher(),
        ];
        BloomFilter {
            bitmap: BitVec::from_elem(optimal_m, false),
            optimal_m,
            optimal_k,
            hashers,
        }
    }

    /// Insert item to the set.
    pub fn insert(&mut self, item: &str) {
        let (h1, h2) = self.hash_kernel(item);

        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);

            self.bitmap.set(index, true);
        }
    }

    /// Check if an item is present in the set.
    /// There can be false positives, but no false negatives.
    pub fn contains(&self, item: &str) -> bool {
        let (h1, h2) = self.hash_kernel(item);

        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);

            if !self.bitmap.get(index).unwrap() {
                return false;
            }
        }

        true
    }

    /// Get the index from hash value of `k_i`.
    fn get_index(&self, h1: u64, h2: u64, k_i: u64) -> usize {
        h1.wrapping_add((k_i).wrapping_mul(h2)) as usize % self.optimal_m
    }

    /// Calculate the size of `bitmap`.
    /// The size of bitmap depends on the target false positive probability
    /// and the number of items in the set.
    fn bitmap_size(items_count: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items_count as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }

    /// Calculate the number of hash functions.
    /// The required number of hash functions only depends on the target
    /// false positive probability.
    fn optimal_k(fp_rate: f64) -> u32 {
        ((-1.0f64 * fp_rate.ln()) / core::f64::consts::LN_2).ceil() as u32
    }

    /// Calculate two hash values from which the k hashes are derived.
    fn hash_kernel(&self, item: &str) -> (u64, u64) {
        let hasher1 = &mut self.hashers[0].clone();
        let hasher2 = &mut self.hashers[1].clone();

        item.hash(hasher1);
        item.hash(hasher2);

        let hash1 = hasher1.finish();
        let hash2 = hasher2.finish();

        (hash1, hash2)
    }
}
