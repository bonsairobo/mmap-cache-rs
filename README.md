# mmap-cache

A low-level API for a memory-mapped cache of a read-only key-value store.

### Example

```rust
use mmap_cache::{FileBuilder, MmapCache};

const INDEX_PATH: &str = "/tmp/mmap_cache_index";
const VALUES_PATH: &str = "/tmp/mmap_cache_values";

// Serialize to files.
let mut builder = FileBuilder::create_files(INDEX_PATH, VALUES_PATH)?;
builder.insert(b"foo", b"bar")?;
builder.finish()?;

// Map the files into memory.
let cache = unsafe { MmapCache::map_paths(INDEX_PATH, VALUES_PATH) }?;
let offset = cache.get_value_offset(b"foo").unwrap() as usize;
let value: &[u8; 3] = unsafe { std::mem::transmute(&cache.value_bytes()[offset]) };
assert_eq!(value, b"bar");
```
