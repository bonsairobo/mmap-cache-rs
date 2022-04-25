mod builder;
mod cache;
mod error;

pub use builder::*;
pub use cache::*;
pub use error::*;

pub use fst;
pub use memmap;

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::cast_slice;
    use fst::Streamer;

    #[test]
    fn serialize_and_read() {
        const PAIRS: [(&[u8], [i32; 3]); 4] = [
            (b"cat", [1, 2, 3]),
            (b"dog", [2, 3, 4]),
            (b"frog", [3, 4, 5]),
            (b"goose", [4, 5, 6]),
        ];

        let index_path = "/tmp/mmap_cache_index";
        let values_path = "/tmp/mmap_cache_values";

        let mut builder = FileBuilder::create_files(index_path, values_path).unwrap();
        for (key, value) in PAIRS {
            builder.insert(key, cast_slice(&value)).unwrap();
        }
        builder.finish().unwrap();

        let cache = unsafe { MmapCache::map_paths(index_path, values_path) }.unwrap();
        let dog: &[u8] = b"dog";
        let frog: &[u8] = b"frog";
        let mut stream = cache.range_stream(dog..=frog);
        let mut key_values = Vec::new();
        while let Some((key, offset)) = stream.next() {
            let value: &[i32; 3] = unsafe { cache.value_at_offset(offset) };
            key_values.push((key.to_vec(), value));
        }

        assert_eq!(
            key_values,
            [(dog.to_vec(), &[2, 3, 4]), (frog.to_vec(), &[3, 4, 5])]
        );
    }
}
