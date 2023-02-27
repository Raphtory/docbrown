#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

pub mod csv_loader;
pub mod data;
pub mod graph;
pub mod graph_window;
pub mod polars_loader;
