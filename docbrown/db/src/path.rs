use crate::edge::EdgeView;
use crate::path::Operations::Neighbours;
use crate::vertex::VertexView;
use crate::view_api::{GraphViewOps, VertexListOps, VertexViewOps};
use docbrown_core::tgraph::VertexRef;
use docbrown_core::{Direction, Prop};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::iter;
use std::iter::Map;
use std::sync::Arc;

#[derive(Copy, Clone)]
pub(crate) enum Operations {
    Neighbours {
        dir: Direction,
    },
    NeighboursWindow {
        dir: Direction,
        t_start: i64,
        t_end: i64,
    },
}

impl Operations {
    fn op<G: GraphViewOps>(
        self,
        graph: G,
        iter: Box<dyn Iterator<Item = VertexRef> + Send>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        match self {
            Operations::Neighbours { dir } => {
                Box::new(iter.flat_map(move |v| graph.neighbours(v, dir)))
            }
            Operations::NeighboursWindow {
                dir,
                t_start,
                t_end,
            } => Box::new(iter.flat_map(move |v| graph.neighbours_window(v, t_start, t_end, dir))),
        }
    }
}

pub struct PathFromGraph<G: GraphViewOps> {
    graph: G,
    operations: Arc<Vec<Operations>>,
}

impl<G: GraphViewOps> PathFromGraph<G> {
    fn iter(&self) -> Box<dyn Iterator<Item = PathFromVertex<G>> + Send> {
        let g = self.graph.clone();
        let ops = self.operations.clone();
        Box::new(self.graph.vertex_refs().map(move |v| PathFromVertex {
            graph: g.clone(),
            vertex: v,
            operations: ops.clone(),
        }))
    }

    fn id(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.id()))
    }

    fn prop(
        &self,
        name: String,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.prop(name.clone())))
    }

    fn props(
        &self,
    ) -> Box<
        dyn Iterator<Item = Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send>>
            + Send,
    > {
        Box::new(self.iter().map(|it| it.props()))
    }

    fn degree(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.degree()))
    }

    fn degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(move |it| it.degree_window(t_start, t_end)))
    }

    fn in_degree(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.in_degree()))
    }

    fn in_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.in_degree_window(t_start, t_end)),
        )
    }

    fn out_degree(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(self.iter().map(|it| it.out_degree()))
    }

    fn out_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = usize> + Send>> + Send> {
        Box::new(
            self.iter()
                .map(move |it| it.out_degree_window(t_start, t_end)),
        )
    }

    fn edges(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(|it| it.edges()))
    }

    fn edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(move |it| it.edges_window(t_start, t_end)))
    }

    fn in_edges(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(|it| it.in_edges()))
    }

    fn in_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(
            self.iter()
                .map(move |it| it.in_edges_window(t_start, t_end)),
        )
    }

    fn out_edges(&self) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(self.iter().map(|it| it.out_edges()))
    }

    fn out_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>>> {
        Box::new(
            self.iter()
                .map(move |it| it.out_edges_window(t_start, t_end)),
        )
    }

    fn neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours { dir: Direction::IN });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::IN,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }

    fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            operations: Arc::new(new_ops),
        }
    }
}

pub struct PathFromVertex<G: GraphViewOps> {
    graph: G,
    vertex: VertexRef,
    operations: Arc<Vec<Operations>>,
}

impl<G: GraphViewOps> PathFromVertex<G> {
    fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let init: Box<dyn Iterator<Item = VertexRef> + Send> = Box::new(iter::once(self.vertex));
        let g = self.graph.clone();
        let ops = self.operations.clone();
        Box::new(
            ops.iter()
                .fold(init, |it, op| Box::new(op.op(g.clone(), it)))
                .map(move |v| VertexView::new(g.clone(), v)),
        )
    }

    fn id(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.iter().id()
    }

    fn prop(&self, name: String) -> Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send> {
        self.iter().prop(name)
    }

    fn props(&self) -> Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send> {
        self.iter().props()
    }

    fn degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().degree()
    }

    fn degree_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().degree_window(t_start, t_end)
    }

    fn in_degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().in_degree()
    }

    fn in_degree_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().in_degree_window(t_start, t_end)
    }

    fn out_degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().out_degree()
    }

    fn out_degree_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().out_degree_window(t_start, t_end)
    }

    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().edges()
    }

    fn edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().edges_window(t_start, t_end)
    }

    fn in_edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().in_edges()
    }

    fn in_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().in_edges_window(t_start, t_end)
    }

    fn out_edges(&self) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().out_edges()
    }

    fn out_edges_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeView<G>> + Send> {
        self.iter().out_edges_window(t_start, t_end)
    }

    fn neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::BOTH,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn in_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours { dir: Direction::IN });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::IN,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn out_neighbours(&self) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::Neighbours {
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }

    fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> Self {
        let mut new_ops = (*self.operations).clone();
        new_ops.push(Operations::NeighboursWindow {
            t_start,
            t_end,
            dir: Direction::OUT,
        });
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            operations: Arc::new(new_ops),
        }
    }
}
