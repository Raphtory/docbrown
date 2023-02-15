use serde::{Deserialize, Serialize};

use crate::tadjset::{AdjEdge, TAdjSet};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) enum Adj {
    Solo(u64),
    List {
        logical: u64,
        out: TAdjSet<usize, i64>,         // local
        into: TAdjSet<usize, i64>,        // local
        remote_out: TAdjSet<usize, i64>,  // remote
        remote_into: TAdjSet<usize, i64>, // remote
    },
}

impl Adj {
    pub(crate) fn new_out(g_v_id: u64, v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                logical: g_v_id,
                out: TAdjSet::new(t, v, e),
                into: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                logical: g_v_id,
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_out: TAdjSet::new(t, v, e),
                remote_into: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn new_into(g_v_id: u64, v: usize, t: i64, e: AdjEdge) -> Self {
        if e.is_local() {
            Adj::List {
                logical: g_v_id,
                into: TAdjSet::new(t, v, e),
                out: TAdjSet::default(),
                remote_out: TAdjSet::default(),
                remote_into: TAdjSet::default(),
            }
        } else {
            Adj::List {
                logical: g_v_id,
                out: TAdjSet::default(),
                into: TAdjSet::default(),
                remote_into: TAdjSet::new(t, v, e),
                remote_out: TAdjSet::default(),
            }
        }
    }

    pub(crate) fn logical(&self) -> &u64 {
        match self {
            Adj::Solo(logical) => logical,
            Adj::List { logical, .. } => logical,
        }
    }

    pub(crate) fn out_edges_len(&self) -> usize {
        match self {
            Adj::Solo(_) => 0,
            Adj::List {
                out, remote_out, ..
            } => out.len() + remote_out.len(),
        }
    }

    pub(crate) fn in_edges_len(&self) -> usize {
        match self {
            Adj::Solo(_) => 0,
            Adj::List {
                into, remote_into, ..
            } => into.len() + remote_into.len(),
        }
    }
}
