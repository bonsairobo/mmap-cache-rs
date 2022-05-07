use crate::Error;

use std::fs;
use std::io;
use std::path::Path;

/// Serializes a stream of `([u8], [u8])` key-value pairs.
///
/// Serialization happens by writing key-value pairs in sorted order. A value is always written before its corresponding key,
/// because the index will map that key to the starting byte offset of the value that was written.
///
/// Many calls of `append_value_bytes` can be made before committing the key --> offset mapping:
///
/// ```
/// # use mmap_cache::Error;
/// # fn example() -> Result<(), Error> {
/// use mmap_cache::{Builder, Cache};
///
/// let mut index_bytes = Vec::new();
/// let mut value_bytes = Vec::new();
///
/// let mut builder = Builder::new(&mut index_bytes, &mut value_bytes)?;
///
/// // Write a value with multiple append calls.
/// builder.append_value_bytes(&777u32.to_be_bytes())?;
/// builder.append_value_bytes(&777u32.to_be_bytes())?;
/// builder.commit_entry(b"hot_garbage")?;
///
/// // Or equivalently, use just one insert call.
/// let mut buf = [0; 8];
/// buf[0..4].copy_from_slice(&777u32.to_be_bytes());
/// buf[4..8].copy_from_slice(&777u32.to_be_bytes());
/// builder.insert(b"lots_of_garbage", &buf)?;
///
/// builder.finish()?;
///
/// let cache = Cache::new(&index_bytes, &value_bytes)?;
/// assert_eq!(cache.get_value_offset(b"hot_garbage"), Some(0));
/// assert_eq!(unsafe { cache.get_transmuted_value(b"hot_garbage") }, Some(&buf));
/// assert_eq!(cache.get_value_offset(b"lots_of_garbage"), Some(8));
/// assert_eq!(unsafe { cache.get_transmuted_value(b"lots_of_garbage") }, Some(&buf));
/// # Ok(())
/// # }
/// # example().unwrap();
/// ```
pub struct Builder<WK, WV> {
    map_builder: fst::MapBuilder<WK>,
    value_writer: WV,
    value_cursor: usize,
    committed_value_cursor: usize,
}

impl<WK, WV> Builder<WK, WV>
where
    WK: io::Write,
    WV: io::Write,
{
    /// Creates a new [`Builder`] for serializing a collection of key-value pairs.
    ///
    /// - `index_writer`: Writes the serialized [`fst::Map`] which stores the value offsets.
    /// - `value_writer`: Writes the values pointed to by the byte offsets stored in the [`fst::Map`].
    ///
    /// ## Warning
    ///
    /// This crate has no control over the alignment guarantees provided by the given writers. Be careful to preserve alignment
    /// when using [`memmap2`].
    pub fn new(index_writer: WK, value_writer: WV) -> Result<Self, Error> {
        Ok(Self {
            map_builder: fst::MapBuilder::new(index_writer)?,
            value_writer,
            committed_value_cursor: 0,
            value_cursor: 0,
        })
    }

    /// Writes `value` into the value stream and commits the entry, storing the value's [`u64`] byte offset along with the `key`
    /// in the [`fst::Map`].
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.append_value_bytes(value)?;
        self.commit_entry(key)
    }

    /// Finishes writing the current value, associating the starting byte offset of the value with `key`.
    pub fn commit_entry(&mut self, key: &[u8]) -> Result<(), Error> {
        self.map_builder
            .insert(key, u64::try_from(self.committed_value_cursor).unwrap())?;
        self.committed_value_cursor = self.value_cursor;
        Ok(())
    }

    /// Writes `value` into the value stream.
    ///
    /// The caller may continue appending more value bytes as needed before calling `commit_entry` to finish the current entry
    /// and start a new one.
    pub fn append_value_bytes(&mut self, value: &[u8]) -> Result<(), Error> {
        self.value_writer.write_all(value)?;
        self.value_cursor += value.len();
        Ok(())
    }

    /// Completes the serialization and flushes any outstanding IO.
    pub fn finish(mut self) -> Result<(), Error> {
        self.value_writer.flush()?;
        Ok(self.map_builder.finish()?)
    }
}

pub type FileBuilder = Builder<io::BufWriter<fs::File>, io::BufWriter<fs::File>>;

impl FileBuilder {
    /// Creates a new [`Builder`], using the file at `index_path` for an index writer and the file at `value_path` as a value
    /// writer.
    ///
    /// This always overwrites the given files.
    ///
    /// After calling `finish`, these same files can be used with `Cache::map_files`.
    pub fn create_files(
        index_path: impl AsRef<Path>,
        value_path: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let index_writer = io::BufWriter::new(fs::File::create(index_path)?);
        let value_writer = io::BufWriter::new(fs::File::create(value_path)?);
        Builder::new(index_writer, value_writer)
    }
}
