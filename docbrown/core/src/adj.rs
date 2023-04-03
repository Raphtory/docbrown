use serde::{Deserialize, Serialize};

use crate::tadjset::{AdjEdge, TAdjSet};

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub(crate) enum Adj {
    #[default]
    Solo,
    List {
        out: TAdjSet<usize, i64>,         // local
        into: TAdjSet<usize, i64>,        // local
        remote_out: TAdjSet<usize, i64>,  // remote
        remote_into: TAdjSet<usize, i64>, // remote
    },
}

impl Default for &Adj {
    fn default() -> Self {
        static DEFAULT: Adj = Adj::Solo;
        &DEFAULT
    }
}

impl Adj {
    pub(crate) fn new_out(v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                out: TAdjSet::new(t, v, e),
                into: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_out: TAdjSet::new(t, v, e),
                remote_into: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn new_into(v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                into: TAdjSet::new(t, v, e),
                out: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_into: TAdjSet::new(t, v, e),
                remote_out: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn out_edges_len(&self) -> usize {
        match self {
            Adj::Solo => 0,
            Adj::List {
                out, remote_out, ..
            } => out.len() + remote_out.len(),
        }
    }
}
