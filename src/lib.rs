///
/// A simple disk-backed segmented bloom filter.
///
/// Two files:
///  - Append-only write log
///    - new data (in hash form, I guess), flushed on size limit to:
///  - The bloom filter
///    - Or actually, an array of 16k filters.
///
/// ... or an array of bloom filters.  6MiB would be good for 1.1 million files
/// w/ error rate of 1 in a billion.
///
/// 2 MiB good for ~390k files
/// split into 256 8KiB pages of ~1500 each
/// 16 KiB pages would fit in a bitmap of 128 bits.
use std::fs::OpenOptions;
use std::hash::Hash;

use bitvec_rs::BitVec;
use siphasher::sip128::{Hasher128, SipHasher};

mod params;
pub use params::*;

#[derive(Debug, Clone, Copy)]
pub struct BloomHash {
    h1: u64,
    h2: u64,
}

impl<T> From<T> for BloomHash
where
    T: Hash,
{
    fn from(hashable: T) -> Self {
        let mut hash = SipHasher::new();
        hashable.hash(&mut hash);
        let h = hash.finish128();

        Self { h1: h.h1, h2: h.h2 }
    }
}

impl BloomHash {
    fn nth(&self, i: u32) -> u64 {
        self.h1.wrapping_add(u64::from(i).wrapping_mul(self.h2))
    }
}

#[derive(Debug)]
pub struct BloomFilter {
    params: BloomFilterParams,
    count: u32,
    pages: u32,
    dirty: BitVec,
    filter: BitVec,
}

const BLOOM_PAGE_SIZE: u32 = 1024 * 16;
const BLOOM_PAGE_BIT_SIZE: u32 = BLOOM_PAGE_SIZE * 8;

impl BloomFilter {
    pub fn from_params(params: BloomFilterParams) -> Self {
        // round to the nearest page size and recalculate our capacity etc
        let params = BloomFilterParamsBuilder::default()
            .bits(params.m + BLOOM_PAGE_BIT_SIZE - (params.m % BLOOM_PAGE_BIT_SIZE))
            .false_positives(params.p)
            .to_params()
            .unwrap();

        let pages = params.m / BLOOM_PAGE_BIT_SIZE;

        Self {
            dirty: BitVec::from_elem(pages as usize, false),
            filter: BitVec::from_elem(params.m as usize, false),
            count: 0,
            pages,
            params,
        }
    }

    pub fn with_capacity_p(capacity: u32, p: f64) -> Self {
        Self::from_params(BloomFilterParams::with_capacity_p(capacity, p))
    }

    pub fn contains<T: Into<BloomHash>>(&mut self, item: T) -> bool {
        self.check_or_insert(item.into(), false)
    }

    pub fn checked_insert<T: Into<BloomHash>>(&mut self, item: T) -> Option<bool> {
        if self.is_full() {
            None
        } else {
            Some(self.check_or_insert(item.into(), true))
        }
    }

    pub fn insert<T: Into<BloomHash>>(&mut self, item: T) -> bool {
        self.check_or_insert(item.into(), true)
    }

    fn check_or_insert(&mut self, hash: BloomHash, insert: bool) -> bool {
        let page = if self.pages > 0 {
            (hash.nth(self.params.k + 1) % u64::from(self.pages))
        } else {
            0
        };

        let offset = page * u64::from(BLOOM_PAGE_BIT_SIZE);

        assert!(offset + u64::from(BLOOM_PAGE_BIT_SIZE) <= self.filter.len() as u64);

        let mut added = false;

        for k in 0..self.params.k {
            let bit = offset + (hash.nth(k) % u64::from(BLOOM_PAGE_BIT_SIZE));

            if !self.filter.get(bit as usize).expect("within bounds") {
                if !insert {
                    return false;
                }

                added = true;

                self.filter.set(bit as usize, true);
            }
        }

        if !insert {
            return true;
        }

        if added {
            self.count += 1;
            self.dirty.set(page as usize, true);
        }

        added
    }

    pub fn count_estimate(&self) -> u32 {
        -((f64::from(self.params.m) / f64::from(self.params.k))
            * (1.0 - (f64::from(self.count_ones()) / f64::from(self.params.m))).ln()) as u32
    }

    fn count_ones(&self) -> u32 {
        self.filter.as_bytes().iter().map(|b| b.count_ones()).sum()
    }

    pub fn is_full(&self) -> bool {
        self.count >= self.params.n
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl From<BloomFilterParams> for BloomFilter {
    fn from(p: BloomFilterParams) -> Self {
        Self::from_params(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloomfilter_looks_reasonable() {
        let mut bf = BloomFilter::with_capacity_p(400, 0.01);

        assert_eq!(false, bf.contains("meep"));
        assert_eq!(false, bf.contains(BloomHash::from("meep")));

        assert_eq!(true, bf.insert("meep"));
        assert_eq!(false, bf.insert("meep"));
        assert_eq!(true, bf.contains(BloomHash::from("meep")));

        assert_eq!(true, bf.insert("moop"));
        assert_eq!(false, bf.insert("moop"));
        assert_eq!(true, bf.contains(BloomHash::from("moop")));
        assert_eq!(2, bf.count_estimate());
    }

    #[test]
    fn bloomfilter_degenerate() {
        let lim = 40000;
        let mut bf = BloomFilter::with_capacity_p(lim, 0.01);

        let mut found = 0;

        for i in 0..lim {
            if !bf.insert(i) {
                found += 1;
            }
        }

        assert!(found < ((lim as f32) * 0.01) as u32);
    }
}
