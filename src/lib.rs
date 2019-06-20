/// Prototypical disk-backed Rust bloom filter
///
/// Todo:
/// * Write log, rather than adding items directly, write serialized BloomHash
///   structs to a log and apply them when the log hits a size limit, to minimise
///   write costs.
///
/// * Multiple reader/writers with eventual consistency.
///
/// * Scalable filters - multiple filters scaled to maintain a desired false-
///   positive rate for an unbounded number of items.
///
/// * Proper tests.
///
/// For my current purposes I ended up just using the write log idea - 16 bytes
/// per entry was sufficient and the implementation was dead simple.

use std::convert::TryInto;
use std::io::{self, Seek, Read, Write};
use std::path::Path;
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
            .bits(params.m + (BLOOM_PAGE_BIT_SIZE - (params.m % BLOOM_PAGE_BIT_SIZE)))
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

    pub fn from_reader<R: Read>(mut reader: R) -> io::Result<Self> {
        let mut header = [0; BLOOM_PAGE_SIZE as usize];
        reader.read_exact(&mut header[..])?;
        assert!(&header[0..8] == b"BLOOMv00");
        let n = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let m = u32::from_be_bytes(header[12..16].try_into().unwrap());
        let k = u32::from_be_bytes(header[16..20].try_into().unwrap());

        let mut filter = vec![0; (m / 8) as usize];
        reader.read_exact(&mut filter[..])?;

        let params = BloomFilterParamsBuilder::default()
            .capacity(n)
            .bits(m)
            .hashes(k)
            .to_params()
            .unwrap();

        let pages = params.m / BLOOM_PAGE_BIT_SIZE;

        let mut ret = Self {
            dirty: BitVec::from_elem(pages as usize, false),
            filter: BitVec::from_bytes(&filter[..]),
            count: 0,
            pages,
            params,
        };

        ret.count = ret.count_estimate();
        Ok(ret)
    }

    pub fn with_capacity_p(capacity: u32, p: f64) -> Self {
        Self::from_params(BloomFilterParams::with_capacity_p(capacity, p))
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::from_reader(std::fs::File::open(path.as_ref())?)
    }

    fn write_header<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(b"BLOOMv00")?;
        writer.write_all(&self.params.n.to_be_bytes())?;
        writer.write_all(&self.params.m.to_be_bytes())?;
        writer.write_all(&self.params.k.to_be_bytes())
    }

    pub fn save<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
        if let Ok(mut file) = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path.as_ref()) {
            let mut header = [0; BLOOM_PAGE_SIZE as usize];
            self.write_header(&mut header[..]).unwrap();

            file.write_all(&header[..])?;
            file.write_all(self.filter.as_bytes())?;
            file.sync_all()?;
            self.clear_dirty();
            return Ok(());
        }

        let mut file = OpenOptions::new().write(true).open(path.as_ref())?;
        let bytes = self.filter.as_bytes();
        for index in self.dirty.iter().enumerate().filter(|(_, bit)| *bit).map(|(index, _)| index) {
            file.seek(io::SeekFrom::Start(((1 + index) * BLOOM_PAGE_SIZE as usize) as u64))?;
            file.write_all(&bytes[(index * BLOOM_PAGE_SIZE as usize)..((index * BLOOM_PAGE_SIZE as usize) + BLOOM_PAGE_SIZE as usize)])?;
        }
        file.sync_all()?;
        self.clear_dirty();

        Ok(())
    }

    fn clear_dirty(&mut self) {
        self.dirty.with_bytes_mut(|buf| buf.iter_mut().for_each(|b| *b = 0));
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
    fn bloomfilter_save_load() {
        let mut bf = BloomFilter::with_capacity_p(1024, 0.01);

        for i in 0..512 {
            assert_eq!(true, bf.insert(i));
        }

        bf.save("test.bf").unwrap();

        let mut bf = BloomFilter::load("test.bf").unwrap();
        for i in 0..512 {
            assert_eq!(true, bf.contains(i));
        }

        assert_eq!(true, bf.insert(513));
        bf.save("test.bf").unwrap();

        let mut bf = BloomFilter::load("test.bf").unwrap();
        for i in 0..512 {
            assert_eq!(true, bf.contains(i));
        }

        assert_eq!(true, bf.contains(513));
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
