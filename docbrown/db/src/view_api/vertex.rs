use crate::edge::{EdgeList, EdgeView};
use crate::path::PathFromVertex;
use crate::vertex::VertexView;
use crate::view_api::edge::EdgeListOps;
use crate::view_api::{GraphViewOps, TimeOps};
use docbrown_core::Prop;
use std::collections::HashMap;

/// Operations defined for a vertex
pub trait VertexViewOps: TimeOps {
    type Graph: GraphViewOps;

    /// Get the numeric id of the vertex
    fn id(&self) -> u64;

    /// Get the name of this vertex if a user has set one otherwise it returns the ID.
    ///
    /// # Returns
    ///
    /// The name of the vertex if one exists, otherwise the ID as a string.
    fn name(&self) -> String;

    /// Get the timestamp for the earliest activity of the vertex
    fn earliest_time(&self) -> Option<i64>;

    /// Get the timestamp for the latest activity of the vertex
    fn latest_time(&self) -> Option<i64>;

    fn property(&self, name: String, include_static: bool) -> Option<Prop>;

    /// Get the temporal property value of this vertex.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    ///
    /// # Returns
    ///
    /// A vector of `(i64, Prop)` tuples where the `i64` value is the timestamp of the
    /// property value and `Prop` is the value itself.
    fn property_history(&self, name: String) -> Vec<(i64, Prop)>;

    fn properties(&self, include_static: bool) -> HashMap<String, Prop>;

    /// Get all temporal property values of this vertex.
    ///
    /// # Returns
    ///
    /// A HashMap with the names of the properties as keys and a vector of `(i64, Prop)` tuples
    /// as values. The `i64` value is the timestamp of the property value and `Prop`
    /// is the value itself.
    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>>;

    fn property_names(&self, include_static: bool) -> Vec<String>;

    fn has_property(&self, name: String, include_static: bool) -> bool;

    fn has_static_property(&self, name: String) -> bool;

    fn static_property(&self, name: String) -> Option<Prop>;

    /// Get the degree of this vertex (i.e., the number of edges that are incident to it).
    ///
    /// # Returns
    ///
    /// The degree of this vertex.
    fn degree(&self) -> usize;

    /// Get the in-degree of this vertex (i.e., the number of edges that point into it).
    ///
    /// # Returns
    ///
    /// The in-degree of this vertex.
    fn in_degree(&self) -> usize;

    /// Get the out-degree of this vertex (i.e., the number of edges that point out of it).
    ///
    /// # Returns
    ///
    /// The out-degree of this vertex.
    fn out_degree(&self) -> usize;

    /// Get the edges that are incident to this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that are incident to this vertex.
    fn edges(&self) -> EdgeList<Self::Graph>;

    /// Get the edges that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point into this vertex.
    fn in_edges(&self) -> EdgeList<Self::Graph>;

    /// Get the edges that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the edges that point out of this vertex.
    fn out_edges(&self) -> EdgeList<Self::Graph>;

    /// Get the neighbours of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex.
    fn neighbours(&self) -> PathFromVertex<Self::Graph>;

    /// Get the neighbours of this vertex that point into this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point into this vertex.
    fn in_neighbours(&self) -> PathFromVertex<Self::Graph>;

    /// Get the neighbours of this vertex that point out of this vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of this vertex that point out of this vertex.
    fn out_neighbours(&self) -> PathFromVertex<Self::Graph>;
}

/// A trait for operations on a list of vertices.
pub trait VertexListOps:
    IntoIterator<Item = Self::Vertex, IntoIter = Self::IterType> + Sized + Send
{
    type Graph: GraphViewOps;
    type Vertex: VertexViewOps<Graph = Self::Graph>;

    /// The type of the iterator for the list of vertices
    type IterType: Iterator<Item = Self::Vertex> + Send;
    /// The type of the iterator for the list of edges
    type EList: EdgeListOps<Graph = Self::Graph>;
    type VList: VertexListOps<Graph = Self::Graph>;
    type ValueIterType<U>: Iterator<Item = U> + Send;

    /// Return the timestamp of the earliest activity.
    fn earliest_time(self) -> Self::ValueIterType<Option<i64>>;

    /// Return the timestamp of the latest activity.
    fn latest_time(self) -> Self::ValueIterType<Option<i64>>;

    /// Create views for the vertices including all events between `t_start` (inclusive) and `t_end` (exclusive)
    fn window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Self::ValueIterType<<Self::Vertex as TimeOps>::WindowedViewType>;

    /// Create views for the vertices including all events until `end` (inclusive)
    fn at(self, end: i64) -> Self::ValueIterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        self.window(i64::MIN, end.saturating_add(1))
    }

    /// Returns the ids of vertices in the list.
    ///
    /// # Returns
    /// The ids of vertices in the list.
    fn id(self) -> Self::ValueIterType<u64>;
    fn name(self) -> Self::ValueIterType<String>;

    fn property(self, name: String, include_static: bool) -> Self::ValueIterType<Option<Prop>>;

    /// Returns an iterator of the values of the given property name
    /// including the times when it changed
    ///
    /// # Arguments
    /// * `name` - The name of the property.
    ///
    /// # Returns
    /// An iterator of the values of the given property name including the times when it changed
    /// as a vector of tuples of the form (time, property).
    fn property_history(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>>;
    fn properties(self, include_static: bool) -> Self::ValueIterType<HashMap<String, Prop>>;

    /// Returns an iterator over all vertex properties.
    ///
    /// # Returns
    /// An iterator over all vertex properties.
    fn property_histories(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>>;
    fn property_names(self, include_static: bool) -> Self::ValueIterType<Vec<String>>;
    fn has_property(self, name: String, include_static: bool) -> Self::ValueIterType<bool>;

    fn has_static_property(self, name: String) -> Self::ValueIterType<bool>;

    fn static_property(self, name: String) -> Self::ValueIterType<Option<Prop>>;

    /// Returns an iterator over the degree of the vertices.
    ///
    /// # Returns
    /// An iterator over the degree of the vertices.
    fn degree(self) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the in-degree of the vertices.
    /// The in-degree of a vertex is the number of edges that connect to it from other vertices.
    ///
    /// # Returns
    /// An iterator over the in-degree of the vertices.
    fn in_degree(self) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the out-degree of the vertices.
    /// The out-degree of a vertex is the number of edges that connects to it from the vertex.
    ///
    /// # Returns
    ///
    /// An iterator over the out-degree of the vertices.
    fn out_degree(self) -> Self::ValueIterType<usize>;

    /// Returns an iterator over the edges of the vertices.
    fn edges(self) -> Self::EList;

    /// Returns an iterator over the incoming edges of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the incoming edges of the vertices.
    fn in_edges(self) -> Self::EList;

    /// Returns an iterator over the outgoing edges of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing edges of the vertices.
    fn out_edges(self) -> Self::EList;

    /// Returns an iterator over the neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the neighbours of the vertices as VertexViews.
    fn neighbours(self) -> Self::VList;

    /// Returns an iterator over the incoming neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the incoming neighbours of the vertices as VertexViews.
    fn in_neighbours(self) -> Self::VList;

    /// Returns an iterator over the outgoing neighbours of the vertices.
    ///
    /// # Returns
    ///
    /// An iterator over the outgoing neighbours of the vertices as VertexViews.
    fn out_neighbours(self) -> Self::VList;
}
