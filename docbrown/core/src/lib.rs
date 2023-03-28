//! # Docbrown Core
//!
//! `docbrown-core` is the core module for the Docbrown library.
//!
//! The Docbrown library is a temporal graph analytics tool, which allows users to create
//! and analyze graph data with time.
//!
//! This crate provides the core data structures and functions for working with temporal graphs,
//! as well as building and evaluating algorithms.
//!
//! **Note** this module is not meant to be used as a standalone crate, but in conjunction with the
//! docbrown_db crate.
//!
//! ## Example
//!
//! ```rust
//! use docbrown_db::graph::Graph;
//! use docbrown_core::algorithms::degree::max_out_degree;
//!
//! let g = Graph::new(1);
//! let vs = vec![
//!   (1, 1, 2),
//!   (2, 1, 3),
//!   (3, 2, 1),
//!   (4, 3, 2),
//!   (5, 1, 4),
//!   (6, 4, 5),
//!   ];
//!
//! for (t, src, dst) in &vs {
//!    g.add_edge(*t, *src, *dst, &vec![]);
//! }
//!
//! let max_out_degree = max_out_degree(&g);
//! println!("max_out_degree: {:?}", max_out_degree);
//! // 3
//! ```
//! ## Supported Platforms
//!
//! `docbrown-core` supports  support for the following platforms:
//!
//! **Note** they must have Rust 1.53 or later.
//!
//!    * `Linux`
//!    * `Windows`
//!    * `macOS`
//!

use serde::{Deserialize, Serialize};

#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

mod adj;
pub mod agg;
mod bitset;
mod lazy_vec;
pub mod lsm;
mod misc;
mod props;
mod sorted_vec_map;
pub mod state;
pub mod tadjset;
mod tcell;
pub mod tgraph;
pub mod tgraph_shard;
mod tprop;
pub mod utils;
pub mod vertex;

/// Denotes the direction of an edge. Can be incoming, outgoing or both.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

/// Denotes the types of properties allowed to be stored in the graph.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
