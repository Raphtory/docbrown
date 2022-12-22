use std::{
    borrow::{Borrow, BorrowMut},
    collections::BinaryHeap,
};

use roaring::RoaringTreemap;

#[derive(Debug, Default)]
pub enum BitSet {
    #[default]
    Empty,
    One(usize),
    Seq(BinaryHeap<usize>),
    Roaring(RoaringTreemap),
}

impl PartialEq for BitSet {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::One(l0), Self::One(r0)) => l0 == r0,
            (Self::Seq(l0), Self::Seq(r0)) => {
                l0.iter().collect::<Vec<_>>() == r0.iter().collect::<Vec<_>>()
            }
            (Self::Roaring(l0), Self::Roaring(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

const SEQ_MAX_SIZE: usize = 64;

impl BitSet {
    pub fn one(i: usize) -> Self {
        BitSet::One(i)
    }

    pub fn push(&mut self, i: usize) {
        match self.borrow() {
            BitSet::Empty => {
                *self = BitSet::One(i);
            }
            BitSet::One(i0) => {
                let mut seq = BinaryHeap::new();
                seq.push(*i0);
                seq.push(i);
                *self = BitSet::Seq(seq);
            }
            BitSet::Seq(seq) => {
                if seq.len() <= SEQ_MAX_SIZE {
                    if let BitSet::Seq(seq_mut) = self.borrow_mut() {
                        seq_mut.push(i);
                    }
                } else {
                    let mut m = RoaringTreemap::default();
                    for i in seq.iter() {
                        m.insert((*i).try_into().unwrap());
                    }
                    m.push(i.try_into().unwrap());
                    *self = BitSet::Roaring(m);
                }
            }
            BitSet::Roaring(_) => {
                if let BitSet::Roaring(seq_mut) = self.borrow_mut() {
                    seq_mut.push(i.try_into().unwrap());
                }
            }
        }
    }

    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            BitSet::Empty => Box::new(std::iter::empty()),
            BitSet::One(i) => Box::new(std::iter::once(*i)),
            BitSet::Seq(seq) => Box::new(seq.iter().map(|i| *i)),
            BitSet::Roaring(m) => Box::new(m.iter().map(|i| {i as usize})),
        }
    }
}

#[cfg(test)]
mod bitset_test {
    use super::*;

    #[test]
    fn push_one() {
        let mut bs = BitSet::default();

        bs.push(3);

        assert_eq!(vec![3], bs.iter().collect::<Vec<_>>())
    }



    #[test]
    fn push_2() {
        let mut bs = BitSet::default();

        bs.push(3);
        bs.push(19);

        assert_eq!(vec![19, 3], bs.iter().collect::<Vec<_>>())
    }


    #[test]
    fn push_66() {
        let mut bs = BitSet::default();

        for i in 0 .. (SEQ_MAX_SIZE +2) {
            bs.push(i);
        }

        let mut actual = bs.iter().collect::<Vec<usize>>();
        actual.sort();
        assert_eq!((0 .. SEQ_MAX_SIZE + 2).collect::<Vec<usize>>(), actual);
    }
}