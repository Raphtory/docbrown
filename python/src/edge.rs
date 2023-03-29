use pyo3::{pyclass, pymethods};
use docbrown_db::edge::EdgeView;
use docbrown_db::view_api::{EdgeViewOps, GraphViewOps};
use itertools::Itertools;
use crate::vertex::PyVertex;
use crate::wrappers::Prop;

#[pyclass(name = "Edge")]
pub struct PyEdge {
    pub(crate) edge: EdgeView<Box<dyn GraphViewOps>>,
}

impl From<EdgeView<Box<dyn GraphViewOps>>> for PyEdge {
    fn from(value: EdgeView<Box<dyn GraphViewOps>>) -> Self {
        Self { edge: value }
    }
}

#[pymethods]
impl PyEdge {
    pub fn __getitem__(&self, name: String) -> Vec<(i64, Prop)> {
        self.prop(name)
    }

    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }

    pub fn id(&self) -> usize {
        self.edge.id()
    }

    fn src(&self) -> PyVertex {
        self.edge.src().into()
    }

    fn dst(&self) -> PyVertex {
        self.edge_w.dst().into()
    }
}
