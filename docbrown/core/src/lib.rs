#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

mod adj;
mod algo;
mod bitset;
pub mod graph;
mod graphview;
pub mod lsm;
mod misc;
mod props;
mod sorted_vec_map;
mod state;
mod tadjset;
mod tcell;
pub mod tpartition;
mod tprop;
mod tpropvec;

// Denotes edge direction
#[derive(Clone, Copy, PartialEq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Prop {
    Str(String),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
}
