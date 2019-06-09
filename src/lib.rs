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
use std::hash::Hash;

use bitvec::{boxed::BitBox, vec::BitVec};
use bitvec::prelude::*;
use siphasher::sip128::{Hasher128, SipHasher};

use std::time::Instant;

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
    dirty: BitBox,
    filter: BitBox,
}

const BLOOM_PAGE_SIZE: u32 = 1024 * 16;

impl BloomFilter {
    pub fn from_params(mut params: BloomFilterParams) -> Self {
        // params.m = (params.m % (BLOOM_PAGE_SIZE * 8)) + params.m;
        let pages = params.m / (BLOOM_PAGE_SIZE * 8);

        let start = Instant::now();
        let dirty = bitvec![0; pages as usize];
        let filter = bitvec![0; params.m as usize];
        // dirty.resize(pages as usize, false);
        // filter.resize(params.m as usize, false);

        println!("Created BitVec with length {} in {:.2?}", params.m, start.elapsed());

        Self {
            dirty: dirty.into_boxed_bitslice(),
            filter: filter.into_boxed_bitslice(),
            count: 0,
            pages,
            params,
        }
    }

    pub fn with_capacity_p(capacity: u32, p: f64) -> Self {
        Self::from_params(BloomFilterParams::with_capacity_p(capacity, p))
    }

    pub fn contains<T: Into<BloomHash>>(&self, item: T) -> bool {
        let hash = item.into();

        let offset = if self.pages > 0 {
            (hash.nth(self.params.k + 1) % u64::from(self.pages)) * u64::from(BLOOM_PAGE_SIZE * 8)
        } else {
            0
        };

        for k in 0..self.params.k {
            let bit = offset + (hash.nth(k) % u64::from(BLOOM_PAGE_SIZE));

            if !self.filter.get(bit as usize).expect("within bounds") {
                return false;
            }
        }

        true
    }

    pub fn insert<T: Into<BloomHash>>(&mut self, item: T) -> bool {
        let hash = item.into();

        let offset = if self.pages > 0 {
            (hash.nth(self.params.k + 1) % u64::from(self.pages)) * u64::from(BLOOM_PAGE_SIZE * 8)
        } else {
            0
        };

        let mut added = false;

        for k in 0..self.params.k {
            let bit = offset + (hash.nth(k) % u64::from(BLOOM_PAGE_SIZE));

            if !self.filter.get(bit as usize).expect("within bounds") {
                added = true;

                self.filter.set(bit as usize, true);
            }
        }

        added
    }

    pub fn count_estimate(&self) -> u32 {
        -((f64::from(self.params.m) / f64::from(self.params.k))
            * (1.0 - (self.filter.count_ones() as f64 / f64::from(self.params.m)))) as u32
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

#[derive(Debug, Default, Clone)]
pub struct BloomFilterParams {
    pub m: u32,
    pub n: u32,
    pub k: u32,
    pub p: f64,
}

#[derive(Debug, Default, Clone)]
pub struct BloomFilterParamsBuilder {
    m: Option<u32>,
    n: Option<u32>,
    k: Option<u32>,
    p: Option<f64>,
}

impl BloomFilterParams {
    pub fn with_capacity_p(capacity: u32, p: f64) -> Self {
        BloomFilterParamsBuilder::default()
            .capacity(capacity)
            .false_positives(p)
            .to_params()
            .unwrap()
    }
}

impl BloomFilterParamsBuilder {
    pub fn capacity(&mut self, capacity: u32) -> &mut Self {
        self.n = Some(capacity);
        self
    }

    pub fn bits(&mut self, bits: u32) -> &mut Self {
        self.m = Some(bits);
        self
    }

    pub fn bytes(&mut self, bytes: u32) -> &mut Self {
        self.bits(bytes * 8)
    }

    pub fn hashes(&mut self, hashes: u32) -> &mut Self {
        self.k = Some(hashes);
        self
    }

    pub fn false_positives(&mut self, fp: f64) -> &mut Self {
        assert!(fp.is_normal());
        assert!(fp.is_sign_positive());

        if fp > 1.0 {
            self.p = Some(1.0 / fp);
        } else {
            self.p = Some(fp);
        }

        self
    }

    #[allow(clippy::many_single_char_names)]
    pub fn to_params(&self) -> Result<BloomFilterParams, ()> {
        use std::f64::consts::LN_2;

        match *self {
            BloomFilterParamsBuilder {
                m: Some(m),
                n: Some(n),
                k: Some(k),
                p: None,
            } => {
                let r = f64::from(m) / f64::from(n);
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            BloomFilterParamsBuilder {
                m: None,
                n: Some(n),
                k: None,
                p: Some(p),
            } => {
                let m = (f64::from(n) * p.ln() / (1.0 / 2.0_f64.powf(LN_2)).ln()).ceil() as u32;
                let r = f64::from(m) / f64::from(n);
                let k = (LN_2 * r).round() as u32;

                Ok(BloomFilterParams { m, n, k, p })
            }
            BloomFilterParamsBuilder {
                m: Some(m),
                n: Some(n),
                k: None,
                p: None,
            } => {
                let r = f64::from(m) / f64::from(n);
                let k = (LN_2 * r).round() as u32;
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            BloomFilterParamsBuilder {
                m: Some(m),
                n: None,
                k: None,
                p: Some(p),
            } => {
                let n = (f64::from(m) * (1.0 / 2.0_f64.powf(LN_2)) / p.ln()).ceil() as u32;
                let r = f64::from(m) / f64::from(n);
                let k = (LN_2 * r).round() as u32;
                let q = f64::exp(-f64::from(k) / r);
                let p = (1.0 - q).powf(f64::from(k));

                Ok(BloomFilterParams { m, n, k, p })
            }
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_capacity_p() {
        let prm = BloomFilterParams::with_capacity_p(100, 0.01);
        assert_eq!(959, prm.m);
        assert_eq!(7, prm.k);

        let prm = BloomFilterParams::with_capacity_p(1_000_000, 0.0001);
        assert_eq!(19170117, prm.m);
        assert_eq!(13, prm.k);
    }

    #[test]
    fn bloomfilter_looks_reasonable() {
        let mut bf = BloomFilter::with_capacity_p(400_000, 1e-9);

        assert!(false == bf.contains("meep"));
        assert!(false == bf.contains(BloomHash::from("meep")));

        assert!(true == bf.insert("meep"));
        assert!(false == bf.insert("meep"));
        assert!(true == bf.contains(BloomHash::from("meep")));
    }
}
