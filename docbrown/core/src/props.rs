use crate::Prop;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::tpropvec::TPropVec;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Props {
    // Mapping between property name and property id
    pub(crate) prop_ids: HashMap<String, usize>,

    // Vector of vertices properties. Each index represents vertex local (physical) id
    pub(crate) vertex_meta: Vec<TPropVec>,

    // Vector of edge properties. Each "signed" index represents an edge id
    pub(crate) edge_meta: Vec<TPropVec>,
}

impl Default for Props {
    fn default() -> Self {
        Self {
            prop_ids: Default::default(),
            vertex_meta: vec![],
            // Signed indices of "edge_meta" vector are used to denote edge ids. In particular, negative
            // and positive indices to denote remote and local edges, respectively. Here we have initialized
            // "edge_meta" with default value of "TPropVec::Empty" occupying the 0th index. The reason
            // being index "0" can be used to denote neither local nor remote edges. It simply breaks this
            // symmetry, hence we ignore it in our representation.
            edge_meta: vec![Default::default()],
        }
    }
}

impl Props {
    pub fn edges_len(&self) -> usize {
        self.edge_meta.len()
    }

    fn get_prop_id(&mut self, name: &str) -> usize {
        match self.prop_ids.get(name) {
            Some(prop_id) => *prop_id,
            None => {
                let id = self.prop_ids.len();
                self.prop_ids.insert(name.to_string(), id);
                id
            }
        }
    }

    pub fn update_vertex_props(&mut self, index: usize, t: i64, props: &Vec<(String, Prop)>) {
        for (name, prop) in props {
            let prop_id = self.get_prop_id(name);

            match self.vertex_meta.get_mut(index) {
                Some(vertex_props) => vertex_props.set(prop_id, t, prop),
                None => {
                    let prop_cell = TPropVec::from(prop_id, t, prop);
                    self.vertex_meta.insert(index, prop_cell)
                }
            }
        }
    }

    pub fn update_edge_props(
        &mut self,
        src_edge_meta_id: usize,
        t: i64,
        props: &Vec<(String, Prop)>,
    ) {
        if props.is_empty() {
            match self.edge_meta.get_mut(src_edge_meta_id) {
                Some(_edge_props) => {}
                None => self.edge_meta.insert(src_edge_meta_id, TPropVec::Empty),
            }
            return;
        }

        for (name, prop) in props {
            let prop_id = self.get_prop_id(name);

            match self.edge_meta.get_mut(src_edge_meta_id) {
                Some(edge_props) => edge_props.set(prop_id, t, prop),
                None => self
                    .edge_meta
                    .insert(src_edge_meta_id, TPropVec::from(prop_id, t, prop)),
            }
        }
    }
}
