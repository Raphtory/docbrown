use crate::view_api::edge::EdgeViewOps;
use crate::view_api::vertex::VertexViewOps;
use docbrown_core::vertex::InputVertex;

/// This trait GraphViewOps defines operations for accessing
/// information about a graph. The trait has associated types
/// that are used to define the type of the vertices, edges
/// and the corresponding iterators.
pub trait GraphViewOps: Send + Sync {
    /// The type of the vertices in the graph.
    type Vertex: VertexViewOps<Edge = Self::Edge>;

    /// : An iterator over vertices.
    type VertexIter: Iterator<Item = Self::Vertex> + Send;

    /// An iterator over all vertices in the graph.
    type Vertices: IntoIterator<Item = Self::Vertex, IntoIter = Self::VertexIter> + Send;

    /// Defines the type of edges that are present in the graph.
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;

    /// An iterator over all edges in the graph.
    type Edges: IntoIterator<Item = Self::Edge>;

    /// Return the number of vertices in the graph.
    fn num_vertices(&self) -> usize;

    /// Return the earliest timestamp in the graph.
    fn earliest_time(&self) -> Option<i64>;

    /// Return the latest timestamp in the graph.
    fn latest_time(&self) -> Option<i64>;

    /// Check if the graph is empty.
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }

    /// Return the number of edges in the graph.
    fn num_edges(&self) -> usize;

    /// Check if the graph contains a vertex `v`.
    fn has_vertex<T: InputVertex>(&self, v: T) -> bool;

    /// Check if the graph contains an edge given a pair of vertices `(src, dst)`.
    fn has_edge<T: InputVertex>(&self, src: T, dst: T) -> bool;

    /// Get a vertex `v`.
    fn vertex<T: InputVertex>(&self, v: T) -> Option<Self::Vertex>;

    /// Return an iterator over all vertices in the graph.
    fn vertices(&self) -> Self::Vertices;

    /// Get an edge `(src, dst)`.
    fn edge<T: InputVertex>(&self, src: T, dst: T) -> Option<Self::Edge>;

    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Self::Edges;

    /// Return an iterator over all vertices in a given shard.
    fn vertices_shard(&self, shard: usize) -> Self::Vertices;
}
