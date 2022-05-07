# mmap-cache

A low-level API for a memory-mapped cache of a read-only key-value store.

### Design

The [`Cache`] index is an [`fst::Map`], which maps from arbitrary byte sequences to [`u64`]. We use the [`u64`] as byte
offsets into a memory-mapped file that stores arbitrary values. This read-only cache is very compact and simple to
construct, requiring only constant memory for serializing arbitrarily large maps.

By using Finite State Transducers from the [`fst`] crate, we get a highly compressed mapping that performs key --> offset
lookups in O(key length) time.

### Example

```rust
use mmap_cache::{FileBuilder, MmapCache};

const INDEX_PATH: &str = "/tmp/mmap_cache_index";
const VALUES_PATH: &str = "/tmp/mmap_cache_values";

// Serialize to files. As required by the finite state transducer (FST) builder,
// keys must be provided in sorted (lexicographical) order.
let mut builder = FileBuilder::create_files(INDEX_PATH, VALUES_PATH)?;
builder.insert(b"abc", b"def")?;
builder.insert(b"foo", b"bar")?;
builder.finish()?;

// Map the files into memory.
let cache = unsafe { MmapCache::map_paths(INDEX_PATH, VALUES_PATH) }?;
let value = unsafe { cache.get_transmuted_value(b"foo") };
assert_eq!(value, Some(b"bar"));
```

### IO Concurrency

When using [`memmap2`] on a large file, it's likely that accessing values from the cache will cause the thread to block in
the operating system scheduler while the page cache is filled from the file system. To achieve concurrency IO up to some
maximum concurrency N, you could dispatch your IOs in a thread pool of N threads.

License: MIT OR Apache 2.0
