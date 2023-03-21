use crate::graph::Graph;
use docbrown_core::tgraph::TemporalGraph;
use docbrown_core::tgraph_shard::ImmutableTGraphShard;
use docbrown_core::{
    tgraph::{EdgeRef, VertexRef},
    utils,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImmutableGraph {
    pub(crate) nr_shards: usize,
    pub(crate) shards: Vec<ImmutableTGraphShard<TemporalGraph>>,
}

#[derive(Debug, PartialEq)]
pub struct UnfreezeFailure;

impl ImmutableGraph {
    fn unfreeze(self) -> Result<Graph, UnfreezeFailure> {
        let mut shards = Vec::with_capacity(self.shards.len());
        for shard in self.shards {
            match shard.unfreeze() {
                Ok(t) => shards.push(t),
                Err(_) => return Err(UnfreezeFailure),
            }
        }
        Ok(Graph {
            nr_shards: self.nr_shards,
            shards,
        })
    }

    fn shard_id(&self, g_id: u64) -> usize {
        utils::get_shard_id_from_global_vid(g_id, self.nr_shards)
    }

    fn get_shard_from_id(&self, g_id: u64) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(g_id)]
    }

    fn get_shard_from_v(&self, v: VertexRef) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(v.g_id)]
    }

    fn get_shard_from_e(&self, e: EdgeRef) -> &ImmutableTGraphShard<TemporalGraph> {
        &self.shards[self.shard_id(e.src_g_id)]
    }
}
