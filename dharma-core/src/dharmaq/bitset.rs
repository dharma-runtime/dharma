#[derive(Clone, Debug)]
pub struct BitSet {
    bits: Vec<u64>,
    len: usize,
}

impl BitSet {
    pub fn new(len: usize) -> Self {
        let words = (len + 63) / 64;
        BitSet {
            bits: vec![0u64; words],
            len,
        }
    }

    pub fn filled(len: usize) -> Self {
        let mut set = BitSet::new(len);
        for word in set.bits.iter_mut() {
            *word = u64::MAX;
        }
        let extra = set.bits.len().saturating_mul(64).saturating_sub(len);
        if extra > 0 {
            if let Some(last) = set.bits.last_mut() {
                *last &= u64::MAX >> extra;
            }
        }
        set
    }

    pub fn set(&mut self, idx: usize) {
        if idx >= self.len {
            return;
        }
        let word = idx / 64;
        let bit = idx % 64;
        self.bits[word] |= 1u64 << bit;
    }

    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }

    pub fn invert_inplace(&mut self) {
        for word in &mut self.bits {
            *word = !*word;
        }
        let extra = self.bits.len().saturating_mul(64).saturating_sub(self.len);
        if extra > 0 {
            if let Some(last) = self.bits.last_mut() {
                *last &= u64::MAX >> extra;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|w| *w == 0)
    }

    pub fn or_inplace(&mut self, other: &BitSet) {
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a |= *b;
        }
    }

    pub fn and_inplace(&mut self, other: &BitSet) {
        for (a, b) in self.bits.iter_mut().zip(other.bits.iter()) {
            *a &= *b;
        }
    }

    pub fn iter_ones(&self) -> BitSetIter<'_> {
        BitSetIter {
            bits: &self.bits,
            word_idx: 0,
            current: self.bits.get(0).copied().unwrap_or(0),
        }
    }
}

pub struct BitSetIter<'a> {
    bits: &'a [u64],
    word_idx: usize,
    current: u64,
}

impl<'a> Iterator for BitSetIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current != 0 {
                let tz = self.current.trailing_zeros() as usize;
                self.current &= self.current - 1;
                return Some(self.word_idx * 64 + tz);
            }
            self.word_idx += 1;
            if self.word_idx >= self.bits.len() {
                return None;
            }
            self.current = self.bits[self.word_idx];
        }
    }
}

pub fn filter_gt_u64(column: &[u8], val: u64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value > val {
            out.set(idx);
        }
    }
}

pub fn filter_gte_u64(column: &[u8], val: u64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value >= val {
            out.set(idx);
        }
    }
}

pub fn filter_lt_u64(column: &[u8], val: u64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value < val {
            out.set(idx);
        }
    }
}

pub fn filter_lte_u64(column: &[u8], val: u64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value <= val {
            out.set(idx);
        }
    }
}

pub fn filter_eq_u64(column: &[u8], val: u64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value == val {
            out.set(idx);
        }
    }
}

pub fn filter_eq_u32(column: &[u8], val: u32, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(4).enumerate() {
        let value = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        if value == val {
            out.set(idx);
        }
    }
}

pub fn filter_eq_u8(column: &[u8], val: u8, out: &mut BitSet) {
    out.clear();
    for (idx, item) in column.iter().enumerate() {
        if *item == val {
            out.set(idx);
        }
    }
}

pub fn filter_gt_i64_bytes(column: &[u8], val: i64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = i64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value > val {
            out.set(idx);
        }
    }
}

pub fn filter_gte_i64_bytes(column: &[u8], val: i64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = i64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value >= val {
            out.set(idx);
        }
    }
}

pub fn filter_lt_i64_bytes(column: &[u8], val: i64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = i64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value < val {
            out.set(idx);
        }
    }
}

pub fn filter_lte_i64_bytes(column: &[u8], val: i64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = i64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value <= val {
            out.set(idx);
        }
    }
}

pub fn filter_eq_i64_bytes(column: &[u8], val: i64, out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(8).enumerate() {
        let value = i64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if value == val {
            out.set(idx);
        }
    }
}

pub fn filter_eq_bytes32(column: &[u8], val: &[u8; 32], out: &mut BitSet) {
    out.clear();
    for (idx, chunk) in column.chunks_exact(32).enumerate() {
        if chunk == val {
            out.set(idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitset_set_and_iter() {
        let mut bs = BitSet::new(130);
        bs.set(0);
        bs.set(64);
        bs.set(129);
        let values: Vec<usize> = bs.iter_ones().collect();
        assert_eq!(values, vec![0, 64, 129]);
    }

    #[test]
    fn filter_gt_sets_bits() {
        let data = vec![1i64, 5, 2, 9];
        let mut bytes = Vec::new();
        for value in data {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let mut bs = BitSet::new(4);
        filter_gt_i64_bytes(&bytes, 4, &mut bs);
        let values: Vec<usize> = bs.iter_ones().collect();
        assert_eq!(values, vec![1, 3]);
    }

    #[test]
    fn filter_gte_u64_sets_bits() {
        let data = vec![1u64, 5, 2, 9];
        let mut bytes = Vec::new();
        for value in data {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let mut bs = BitSet::new(4);
        filter_gte_u64(&bytes, 5, &mut bs);
        let values: Vec<usize> = bs.iter_ones().collect();
        assert_eq!(values, vec![1, 3]);
    }

    #[test]
    fn filter_lte_u64_sets_bits() {
        let data = vec![1u64, 5, 2, 9];
        let mut bytes = Vec::new();
        for value in data {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let mut bs = BitSet::new(4);
        filter_lte_u64(&bytes, 2, &mut bs);
        let values: Vec<usize> = bs.iter_ones().collect();
        assert_eq!(values, vec![0, 2]);
    }

    #[test]
    fn filter_eq_u8_sets_bits() {
        let data = vec![0u8, 1, 1, 0];
        let mut bs = BitSet::new(data.len());
        filter_eq_u8(&data, 1, &mut bs);
        let values: Vec<usize> = bs.iter_ones().collect();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn filled_sets_all_bits() {
        let bs = BitSet::filled(10);
        let values: Vec<usize> = bs.iter_ones().collect();
        assert_eq!(values.len(), 10);
        assert_eq!(values[0], 0);
        assert_eq!(values[9], 9);
    }
}
