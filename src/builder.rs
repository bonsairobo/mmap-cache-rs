use crate::Error;

use std::fs;
use std::io;
use std::path::Path;

/// Serializes a stream of `([u8], [u8])` key-value pairs.
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
    /// when using [`memmap`].
    pub fn new(index_writer: WK, value_writer: WV) -> Result<Self, Error> {
        Ok(Self {
            map_builder: fst::MapBuilder::new(index_writer)?,
            value_writer,
            committed_value_cursor: 0,
            value_cursor: 0,
        })
    }

    /// Writes `value` into the value stream, storing its [`u64`] offset along with the `key` in the [`fst::Map`].
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.append_value_bytes(value)?;
        self.commit_value(key)
    }

    /// Completes the sequence of value bytes written since that last call of `commit_value` or `insert`.
    pub fn commit_value(&mut self, key: &[u8]) -> Result<(), Error> {
        self.map_builder
            .insert(key, u64::try_from(self.committed_value_cursor).unwrap())?;
        self.committed_value_cursor = self.value_cursor;
        Ok(())
    }

    /// Writes `value` into the value stream. Does not modify the index or byte cursor.
    ///
    /// This can be useful for dynamic value encodings. For example, if you want to encode the length of a dynamically sized
    /// value, you can `append_value_bytes(&value_length.to_be_bytes())`.
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

pub type FileBuilder = Builder<fs::File, fs::File>;

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
        let index_file = fs::File::create(index_path)?;
        let value_file = fs::File::create(value_path)?;
        Builder::new(index_file, value_file)
    }
}
