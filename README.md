# Blooming Rust

Yet another Rust [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter),
with a focus on disk-based storage.

This was going to be used for path storage in Compactor, my Windows 10 filesystem
compression tool, but I settled on a simpler structure that better fit my needs.

The basics look like they work, but it's incomplete and badly tested and mainly
here for curiosity's sake.

```rust
use blooming_rust::*;

// 1024 items, 1% false-positive rate
let mut filter = BloomFilter::with_capacity_p(1024, 0.01);

filter.insert("foo");
filter.insert("bar");
filter.save("filter.bloom").unwrap();

let mut filter = BloomFilter::load("filter.bloom").unwrap();
assert!(filter.contains("foo"));
assert!(filter.contains(BloomHash::from("bar")));
assert!(!filter.contains("baz"));
```

The filter accepts anything `Into<BloomHash>`, which accepts anything `Hash`.
This allows for re-using the same hash calculation for multiple filters, which
might be nice for scalable filters.

It also means you can use the same filter for multiple types, which may or may
not be desirable.  If not you may want to modify it to use `PhantomData` to tag
the filters and hashes with a given type.
