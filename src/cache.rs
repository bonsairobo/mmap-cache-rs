use crate::Error;

use fst::IntoStreamer;
use memmap::Mmap;
use std::fs;
use std::mem;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

/// The byte offset of a value in a [`Cache`].
pub type ValueOffset = u64;

/// A cache, mapping `[u8]` keys to `[u8]` values.
///
/// This cache wraps generic byte storage that implements `AsRef<[u8]>`. This is most commonly a memory-mapped file, [`Mmap`].
///
/// For serializing a stream of (key, value) pairs, see [`Builder`](crate::Builder).
pub struct Cache<DK, DV> {
    index: fst::Map<DK>,
    value_bytes: DV,
}

impl<DK, DV> Cache<DK, DV>
where
    DK: AsRef<[u8]>,
    DV: AsRef<[u8]>,
{
    pub fn new(index_bytes: DK, value_bytes: DV) -> Result<Self, Error> {
        Ok(Self {
            index: fst::Map::new(index_bytes)?,
            value_bytes,
        })
    }

    pub fn index(&self) -> &fst::Map<DK> {
        &self.index
    }

    /// Loads the value at the given byte offset.
    ///
    /// Note that this may block in the case of a page fault, and this will be a common scenario for large or cold data sets.
    ///
    /// # Safety
    ///
    /// `offset` must be an offset that points to the start of a `T` value. Valid offsets can be gotten from
    /// `get_value_offset` and `range_stream`.
    pub unsafe fn value_at_offset<T>(&self, offset: ValueOffset) -> &T {
        mem::transmute(&self.value_bytes.as_ref()[offset as usize])
    }

    /// Returns the byte offset of the value for `key`, if it exists.
    ///
    /// The returned offset can be used with the `value_at_offset` method.
    pub fn get_value_offset(&self, key: &[u8]) -> Option<ValueOffset> {
        self.index.get(key)
    }

    /// Returns a streaming iterator over (key, value offset) pairs.
    ///
    /// The offset is a byte offset pointing to the start of the value for that key.
    pub fn range_stream<K, R>(&self, key_range: R) -> fst::map::Stream
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let builder = self.index.range();
        let builder = match key_range.start_bound() {
            Bound::Unbounded => builder,
            Bound::Excluded(b) => builder.gt(b),
            Bound::Included(b) => builder.ge(b),
        };
        let builder = match key_range.end_bound() {
            Bound::Unbounded => builder,
            Bound::Excluded(b) => builder.lt(b),
            Bound::Included(b) => builder.le(b),
        };
        builder.into_stream()
    }
}

pub type MmapCache = Cache<Mmap, Mmap>;

impl MmapCache {
    /// Maps the files at `index_path` and `value_path` to read-only virtual memory ranges.
    pub unsafe fn map_paths(
        index_path: impl AsRef<Path>,
        value_path: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let index_file = fs::File::open(index_path)?;
        let value_file = fs::File::open(value_path)?;
        Self::map_files(&index_file, &value_file)
    }

    /// Maps`index_file` and `value_file` to read-only virtual memory ranges.
    pub unsafe fn map_files(index_file: &fs::File, value_file: &fs::File) -> Result<Self, Error> {
        let index_mmap = Mmap::map(index_file)?;
        let value_mmap = Mmap::map(value_file)?;
        Self::new(index_mmap, value_mmap)
    }
}
