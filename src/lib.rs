//! A low-level API for a memory-mapped cache of a read-only key-value store.
//!
//! ## Design
//!
//! The [`Cache`] index is an [`fst::Map`], which maps from arbitrary byte sequences to [`u64`]. We use the [`u64`] as byte
//! offsets into a memory-mapped file that stores arbitrary values. This read-only cache is very compact and simple to
//! construct, requiring only constant memory for serializing arbitrarily large maps.
//!
//! By using Finite State Transducers from the [`fst`] crate, we get a highly compressed mapping that performs key --> offset
//! lookups in O(key length) time.
//!
//! ## Example
//!
//! ```
//! # use mmap_cache::Error;
//! # fn example() -> Result<(), Error> {
//! use mmap_cache::{FileBuilder, MmapCache};
//!
//! const INDEX_PATH: &str = "/tmp/mmap_cache_index";
//! const VALUES_PATH: &str = "/tmp/mmap_cache_values";
//!
//! // Serialize to files. As required by the finite state transducer (FST) builder,
//! // keys must be provided in sorted (lexicographical) order.
//! let mut builder = FileBuilder::create_files(INDEX_PATH, VALUES_PATH)?;
//! builder.insert(b"abc", b"def")?;
//! builder.insert(b"foo", b"bar")?;
//! builder.finish()?;
//!
//! // Map the files into memory.
//! let cache = unsafe { MmapCache::map_paths(INDEX_PATH, VALUES_PATH) }?;
//! let value = unsafe { cache.get_transmuted_value(b"foo") };
//! assert_eq!(value, Some(b"bar"));
//! # Ok(())
//! # }
//! # example().unwrap();
//! ```
//!
//! ## IO Concurrency
//!
//! When using [`memmap2`] on a large file, it's likely that accessing values from the cache will cause the thread to block in
//! the operating system scheduler while the page cache is filled from the file system. To achieve IO concurrency up to some
//! maximum concurrency N, you could dispatch your IOs in a thread pool of N threads.

mod builder;
mod cache;
mod error;

pub use builder::*;
pub use cache::*;
pub use error::*;

pub use fst;
pub use memmap2;

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::cast_slice;
    use fst::{IntoStreamer, Streamer};

    #[test]
    fn serialize_and_read_range() {
        serialize_example();

        let cache = unsafe { MmapCache::map_paths(INDEX_PATH, VALUES_PATH) }.unwrap();
        let dog: &[u8] = b"dog";
        let gator: &[u8] = b"gator";
        let mut stream = cache.range(dog..=gator).into_stream();
        let mut key_values = Vec::new();
        while let Some((key, offset)) = stream.next() {
            let offset = offset as usize;
            let value: &[i32; 3] = unsafe { std::mem::transmute(&cache.value_bytes()[offset]) };
            key_values.push((key.to_vec(), value));
        }

        assert_eq!(
            key_values,
            [
                (b"dog".to_vec(), &PAIRS[1].1),
                (b"doggy".to_vec(), &PAIRS[2].1),
                (b"frog".to_vec(), &PAIRS[3].1)
            ]
        );
    }

    #[test]
    fn key_lookups() {
        serialize_example();

        let cache = unsafe { MmapCache::map_paths(INDEX_PATH, VALUES_PATH) }.unwrap();

        let (first_key, first_offset) = cache.first().unwrap();
        assert_eq!(&first_key, b"cat");
        assert_eq!(first_offset, 0);

        let (last_key, last_offset) = cache.last().unwrap();
        assert_eq!(&last_key, b"goose");
        assert_eq!(last_offset, 48);

        // Equal.
        let (le_key, le_offset) = cache.last_le::<4>(b"frog").unwrap();
        assert_eq!(&le_key, b"frog");
        assert_eq!(le_offset, 36);

        // Lesser, same length.
        let (le_key, le_offset) = cache.last_le::<4>(b"full").unwrap();
        assert_eq!(&le_key, b"frog");
        assert_eq!(le_offset, 36);

        // Lesser, same length, different starting letter.
        let (le_key, le_offset) = cache.last_le::<4>(b"goon").unwrap();
        assert_eq!(&le_key, b"frog");
        assert_eq!(le_offset, 36);

        // Lesser, longer.
        let (le_key, le_offset) = cache.last_le::<4>(b"goony").unwrap();
        assert_eq!(&le_key, b"frog");
        assert_eq!(le_offset, 36);

        // Lesser, longer, superstring.
        let (le_key, le_offset) = cache.last_le::<3>(b"doge").unwrap();
        assert_eq!(le_key.as_ref(), b"dog");
        assert_eq!(le_offset, 12);

        // Lesser, same length, substring matches greater key.
        let (le_key, le_offset) = cache.last_le::<4>(b"goos").unwrap();
        assert_eq!(&le_key, b"frog");
        assert_eq!(le_offset, 36);

        // Lesser, shorter.
        let (le_key, le_offset) = cache.last_le::<4>(b"fry").unwrap();
        assert_eq!(&le_key, b"frog");
        assert_eq!(le_offset, 36);
        let (le_key, le_offset) = cache.last_le::<3>(b"do").unwrap();
        assert_eq!(le_key.as_ref(), b"cat");
        assert_eq!(le_offset, 0);
        let (le_key, le_offset) = cache.last_le::<5>(b"food").unwrap();
        assert_eq!(le_key.as_ref(), b"doggy");
        assert_eq!(le_offset, 24);

        // Lesser, shorter, substring matches greater key.
        let (le_key, le_offset) = cache.last_le::<5>(b"fro").unwrap();
        assert_eq!(&le_key, b"doggy");
        assert_eq!(le_offset, 24);

        // No LE keys.
        let result = cache.last_le::<4>(b"candy");
        assert_eq!(result, None);
    }

    const INDEX_PATH: &str = "/tmp/mmap_cache_index";
    const VALUES_PATH: &str = "/tmp/mmap_cache_values";

    const PAIRS: [(&[u8], [i32; 3]); 5] = [
        (b"cat", [1, 2, 3]),
        (b"dog", [2, 3, 4]),
        (b"doggy", [3, 4, 5]),
        (b"frog", [4, 5, 6]),
        (b"goose", [5, 6, 7]),
    ];

    fn serialize_example() {
        let mut builder = FileBuilder::create_files(INDEX_PATH, VALUES_PATH).unwrap();
        for (key, value) in PAIRS {
            builder.insert(key, cast_slice(&value)).unwrap();
        }
        builder.finish().unwrap();
    }
}
