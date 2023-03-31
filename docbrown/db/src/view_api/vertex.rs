use crate::vertex::VertexView;
use crate::view_api::edge::EdgeListOps;
use crate::view_api::GraphViewOps;
use docbrown_core::Prop;
use std::collections::HashMap;

pub trait VertexListOps:
    IntoIterator<Item = VertexView<Self::Graph>, IntoIter = Self::IterType> + Sized + Send
{
    type Graph: GraphViewOps;
    type IterType: Iterator<Item = VertexView<Self::Graph>> + Send;
    type EList: EdgeListOps<Graph = Self::Graph>;
    type ValueIterType<U>: Iterator<Item = U> + Send;

    fn id(self) -> Self::ValueIterType<u64>;

    fn prop(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>>;

    fn props(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>;

    fn degree(self) -> Self::ValueIterType<usize>;

    fn degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize>;

    fn in_degree(self) -> Self::ValueIterType<usize>;

    fn in_degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize>;

    fn out_degree(self) -> Self::ValueIterType<usize>;

    fn out_degree_window(self, t_start: i64, t_end: i64) -> Self::ValueIterType<usize>;

    fn edges(self) -> Self::EList;

    fn edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    fn in_edges(self) -> Self::EList;

    fn in_edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    fn out_edges(self) -> Self::EList;

    fn out_edges_window(self, t_start: i64, t_end: i64) -> Self::EList;

    fn neighbours(self) -> Self;

    fn neighbours_window(self, t_start: i64, t_end: i64) -> Self;

    fn in_neighbours(self) -> Self;

    fn in_neighbours_window(self, t_start: i64, t_end: i64) -> Self;

    fn out_neighbours(self) -> Self;

    fn out_neighbours_window(self, t_start: i64, t_end: i64) -> Self;
}
