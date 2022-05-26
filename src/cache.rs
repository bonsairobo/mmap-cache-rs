use crate::Error;

use fst::raw::Node;
use fst::raw::Transition;
use fst::Streamer;
use memmap2::Mmap;
use std::cmp::Ordering;
use std::fs;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

/// A cache, mapping `[u8]` keys to `[u8]` values.
///
/// This cache wraps generic byte storage that implements `AsRef<[u8]>`. This is most commonly a memory-mapped file, [`Mmap`].
///
/// For serializing a stream of (key, value) pairs, see [`SortedBuilder`](crate::SortedBuilder).
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

    /// Access the internal [`fst::Map`] used for mapping keys to value offsets.
    pub fn index(&self) -> &fst::Map<DK> {
        &self.index
    }

    /// The entire byte slice storing all values.
    pub fn value_bytes(&self) -> &[u8] {
        self.value_bytes.as_ref()
    }

    /// Returns the byte offset of the value for `key`, if it exists.
    ///
    /// The returned offset can be used with the `value_at_offset` method.
    pub fn get_value_offset(&self, key: &[u8]) -> Option<u64> {
        self.index.get(key)
    }

    /// Transmutes the bytes starting at `offset` into a `T` reference.
    ///
    /// # Safety
    ///
    /// `offset` must point to a valid representation of `T` in the `value_bytes` region of memory.
    pub unsafe fn offset_transmuted_value<T>(&self, offset: usize) -> &T {
        std::mem::transmute(&self.value_bytes()[offset])
    }

    /// Transmutes the bytes pointed to by `key` (if any) into a `T` reference.
    ///
    /// # Safety
    ///
    /// `key` must point to a valid representation of `T` in the `value_bytes` region of memory.
    pub unsafe fn get_transmuted_value<T>(&self, key: &[u8]) -> Option<&T> {
        self.get_value_offset(key)
            .map(|offset| self.offset_transmuted_value(offset.try_into().unwrap()))
    }

    /// Returns a streaming iterator over (key, value offset) pairs.
    ///
    /// The offset is a byte offset pointing to the start of the value for that key.
    pub fn range<K, R>(&self, key_range: R) -> fst::map::StreamBuilder
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
        match key_range.end_bound() {
            Bound::Unbounded => builder,
            Bound::Excluded(b) => builder.lt(b),
            Bound::Included(b) => builder.le(b),
        }
    }

    /// Returns the (lexicographical) first (key, value) pair.
    ///
    /// # Panics
    ///
    /// If the actual first key is longer than `N`.
    pub fn first<const N: usize>(&self) -> Option<([u8; N], u64)> {
        self.index.stream().next().map(|(k, offset)| {
            let mut key = [0; N];
            key.copy_from_slice(k);
            (key, offset)
        })
    }

    /// Returns the (lexicographical) last (key, value) pair.
    ///
    /// # Panics
    ///
    /// If the actual last key is longer than `N`.
    pub fn last<const N: usize>(&self) -> Option<([u8; N], u64)> {
        let raw = self.index.as_fst();
        let mut key = [0; N];
        let mut n = raw.root();
        let mut i = 0;
        let mut offset = 0;
        while !n.is_final() || !n.is_empty() {
            let last = n.transition(n.len() - 1);
            key[i] = last.inp;
            n = raw.node(last.addr);
            i += 1;
            offset += last.out.value();
        }
        (i == N).then(|| (key, offset))
    }

    /// Finds the (lexicographical) greatest key `k` such that `k <= upper_bound`.
    ///
    /// # Panics
    ///
    /// If the found key is longer than `N`.
    pub fn last_le<const N: usize>(&self, upper_bound: &[u8]) -> Option<([u8; N], u64)> {
        let raw = self.index.as_fst();
        let mut key = [0; N];
        let offset = self.last_le_recursive(raw, upper_bound, LastLeSearch::initial(raw), &mut key);
        offset.map(|o| (key, o))
    }

    fn last_le_recursive<const N: usize>(
        &self,
        raw: &fst::raw::Fst<DK>,
        upper_bound: &[u8],
        state: LastLeSearch,
        key: &mut [u8; N],
    ) -> Option<u64> {
        if let Ordering::Greater = state.parent_ordering {
            return None;
        }

        let le_found = if !state.node.is_empty() {
            match state.parent_ordering {
                Ordering::Greater => unreachable!(),
                Ordering::Equal => {
                    if state.byte_i < upper_bound.len() {
                        // We need to backtrack if the least terminal key is GREATER than upper_bound.
                        find_last_le_transition(state.node, upper_bound[state.byte_i]).and_then(
                            |(t_i, t)| {
                                key[state.byte_i] = t.inp;
                                let next_state = state.next(raw, upper_bound, t);
                                self.last_le_recursive(raw, upper_bound, next_state, key)
                                    .or_else(|| {
                                        // Backtrack. We should only need to move to the next greatest key.
                                        if t_i > 0 {
                                            let t = state.node.transition(t_i - 1);
                                            key[state.byte_i] = t.inp;
                                            let next_state =
                                                state.next_with_ordering(raw, t, Ordering::Less);
                                            self.last_le_recursive(
                                                raw,
                                                upper_bound,
                                                next_state,
                                                key,
                                            )
                                        } else {
                                            None
                                        }
                                    })
                            },
                        )
                    } else {
                        None
                    }
                }
                Ordering::Less => {
                    // We're already LESS, so just take the greatest key we can find.
                    let t = state.node.transition(state.node.len() - 1);
                    key[state.byte_i] = t.inp;
                    let next_state = state.next_with_ordering(raw, t, Ordering::Less);
                    self.last_le_recursive(raw, upper_bound, next_state, key)
                }
            }
        } else {
            None
        };
        le_found.or_else(|| state.node.is_final().then(|| state.offset_sum))
    }
}

struct LastLeSearch<'a> {
    parent_ordering: Ordering,
    byte_i: usize,
    offset_sum: u64,
    node: Node<'a>,
}

impl<'a> LastLeSearch<'a> {
    fn initial<B>(raw: &'a fst::raw::Fst<B>) -> Self
    where
        B: AsRef<[u8]>,
    {
        Self {
            parent_ordering: Ordering::Equal,
            byte_i: 0,
            offset_sum: 0,
            node: raw.root(),
        }
    }

    fn next<B>(&self, raw: &'a fst::raw::Fst<B>, upper_bound: &[u8], t: Transition) -> Self
    where
        B: AsRef<[u8]>,
    {
        self.next_with_ordering(raw, t, t.inp.cmp(&upper_bound[self.byte_i]))
    }

    fn next_with_ordering<B>(
        &self,
        raw: &'a fst::raw::Fst<B>,
        t: Transition,
        ordering: Ordering,
    ) -> Self
    where
        B: AsRef<[u8]>,
    {
        Self {
            parent_ordering: ordering,
            byte_i: self.byte_i + 1,
            node: raw.node(t.addr),
            offset_sum: self.offset_sum + t.out.value(),
        }
    }
}

/// If there are any transitions from `node` whose input byte is LE `upper_bound`, then one of them will be returned. If there
/// are multiple such transitions, the one with the greatest input byte is returned.
fn find_last_le_transition(node: Node, upper_bound: u8) -> Option<(usize, Transition)> {
    // Binary search over the transitions.
    let mut lower = 0;
    let mut upper = node.len();
    while lower != upper {
        let mid = (lower + upper) / 2;

        let t = node.transition(mid);
        if t.inp <= upper_bound {
            if mid == node.len() - 1 {
                // Transition byte is LE our upper_bound, and we're at the last transition, so this is the *last* LE transition.
                return Some((mid, t));
            }

            let next_t = node.transition(mid + 1);
            if next_t.inp > upper_bound {
                // Transition byte is LE our upper_bound, and the next transition byte is *not*, so this is the *last* LE
                // transition.
                return Some((mid, t));
            }

            // Not the last LE transition, so we need to search higher than mid.
            lower = mid + 1;
        } else {
            // Transition too large, search lower than mid.
            upper = mid;
        }
    }
    None
}

pub type MmapCache = Cache<Mmap, Mmap>;

impl MmapCache {
    /// Maps the files at `index_path` and `value_path` to read-only virtual memory ranges.
    ///
    /// # Safety
    ///
    /// See [`Mmap`].
    pub unsafe fn map_paths(
        index_path: impl AsRef<Path>,
        value_path: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        let index_file = fs::File::open(index_path)?;
        let value_file = fs::File::open(value_path)?;
        Self::map_files(&index_file, &value_file)
    }

    /// Maps`index_file` and `value_file` to read-only virtual memory ranges.
    ///
    /// # Safety
    ///
    /// See [`Mmap`].
    pub unsafe fn map_files(index_file: &fs::File, value_file: &fs::File) -> Result<Self, Error> {
        let index_mmap = Mmap::map(index_file)?;
        let value_mmap = Mmap::map(value_file)?;
        Self::new(index_mmap, value_mmap)
    }
}
