use crate::view_api::edge::{EdgeList, EdgeView};
use docbrown_core::Prop;
use std::collections::HashMap;

pub trait VertexView<'a>: Sized {
    type Edge: EdgeView<'a, Vertex = Self>;
    type VList: VertexList<'a, Vertex = Self, Edge = Self::Edge, EList = Self::EList>;
    type EList: EdgeList<'a, Vertex = Self, Edge = Self::Edge>;

    fn id(&self) -> u64;

    fn prop(&self, name: &str) -> Vec<(i64, Prop)>;

    fn props(&self) -> HashMap<String, Vec<(i64, Prop)>>;

    fn degree(&self) -> usize;

    fn in_degree(&self) -> usize;

    fn out_degree(&self) -> usize;

    fn edges(&self) -> Self::EList;

    fn in_edges(&self) -> Self::EList;

    fn out_edges(&self) -> Self::EList;

    fn neighbours(&self) -> Self::VList;

    fn in_neighbours(&self) -> Self::VList;

    fn out_neighbours(&self) -> Self::VList;
}

pub trait VertexList<'a>:
    IntoIterator<Item = Self::Vertex, IntoIter = Self::IntoIterType> + FromIterator<Self::Vertex>
{
    type Vertex: VertexView<'a, Edge = Self::Edge>;
    type Edge: EdgeView<'a, Vertex = Self::Vertex>;
    type EList: EdgeList<'a, Vertex = Self::Vertex, Edge = Self::Edge>;
    type IntoIterType: Iterator<Item = Self::Vertex> + 'a;

    fn id(self) -> Box<dyn Iterator<Item = u64> + 'a> {
        Box::new(self.into_iter().map(|v| v.id()))
    }

    fn prop(self, name: &'a str) -> Box<dyn Iterator<Item = Vec<(i64, Prop)>> + 'a> {
        Box::new(self.into_iter().map(|v| v.prop(name)))
    }

    fn props(self) -> Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + 'a> {
        Box::new(self.into_iter().map(|v| v.props()))
    }

    fn degree(self) -> Box<dyn Iterator<Item = usize> + 'a> {
        Box::new(self.into_iter().map(|v| v.degree()))
    }

    fn in_degree(self) -> Box<dyn Iterator<Item = usize> + 'a> {
        Box::new(self.into_iter().map(|v| v.in_degree()))
    }

    fn out_degree(self) -> Box<dyn Iterator<Item = usize> + 'a> {
        Box::new(self.into_iter().map(|v| v.out_degree()))
    }

    fn edges(self) -> Self::EList {
        Self::EList::from_iter(self.into_iter().flat_map(|v| v.edges().into_iter()))
    }

    fn in_edges(self) -> Self::EList {
        Self::EList::from_iter(self.into_iter().flat_map(|v| v.in_edges().into_iter()))
    }

    fn out_edges(self) -> Self::EList {
        Self::EList::from_iter(self.into_iter().flat_map(|v| v.out_edges().into_iter()))
    }

    fn neighbours(self) -> Self {
        Self::from_iter(self.into_iter().flat_map(|v| v.neighbours().into_iter()))
    }

    fn in_neighbours(self) -> Self {
        Self::from_iter(self.into_iter().flat_map(|v| v.in_neighbours().into_iter()))
    }

    fn out_neighbours(self) -> Self {
        Self::from_iter(
            self.into_iter()
                .flat_map(|v| v.out_neighbours().into_iter()),
        )
    }
}
