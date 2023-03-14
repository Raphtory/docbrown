use crate::tprop_vec::TPropVec;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::meta::MetaVec;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Props {
    // Mapping between meta data name and property id
    pub(crate) meta_ids: HashMap<String, usize>,

    // Mapping between temporal property name and property id
    pub(crate) prop_ids: HashMap<String, usize>,

    // Vector of vertices properties. Each index represents vertex local (physical) id
    pub(crate) vertices: Vec<EntityProps>,

    // Vector of edge properties. Each "signed" index represents an edge id
    pub(crate) edges: Vec<EntityProps>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct EntityProps {
    pub(crate) meta: MetaVec,
    pub(crate) tprops: TPropVec,
}

impl EntityProps {
    fn empty() -> EntityProps {
        EntityProps {
            meta: MetaVec::Empty,
            tprops: TPropVec::Empty,
        }
    }
}

impl Default for EntityProps {
    fn default() -> Self {
        EntityProps::empty()
    }
}

impl Default for Props {
    fn default() -> Self {
        Self {
            meta_ids: Default::default(),
            prop_ids: Default::default(),
            vertices: vec![],
            // Signed indices of "edge_meta" vector are used to denote edge ids. In particular, negative
            // and positive indices to denote remote and local edges, respectively. Here we have initialized
            // "edge_meta" with default value of "TPropVec::Empty" occupying the 0th index. The reason
            // being index "0" can be used to denote neither local nor remote edges. It simply breaks this
            // symmetry, hence we ignore it in our representation.
            edges: vec![Default::default()],
        }
    }
}

impl Props {
    pub fn get_next_available_edge_id(&self) -> usize {
        self.edges.len()
    }

    fn get_prop_id(id_dict: &mut HashMap<String, usize>, name: &str) -> usize {
        match id_dict.get(name) {
            Some(prop_id) => *prop_id,
            None => {
                let id = id_dict.len();
                id_dict.insert(name.to_string(), id);
                id
            }
        }
    }

    fn get_entity_slot(props_storage: &mut Vec<EntityProps>, id: usize) -> &mut EntityProps {
        if props_storage.len() <= id {
            props_storage.insert(id, EntityProps::empty());
        }
        // now props_storage.len() == id + 1:
        props_storage.get_mut(id).unwrap()
    }

    fn upsert_entity_props(id_dict: &mut HashMap<String, usize>, props_storage: &mut Vec<EntityProps>, t: i64, id: usize, props: &Vec<(String, Prop)>) {
        let entity_slot = Self::get_entity_slot(props_storage, id);
        for (name, prop) in props {
            let prop_id = Self::get_prop_id(id_dict, name);
            entity_slot.tprops.set(prop_id, t, prop);
        }
    }

    pub fn upsert_vertex_props(&mut self, t: i64, vertex_id: usize, props: &Vec<(String, Prop)>) {
        Self::upsert_entity_props(&mut self.prop_ids, &mut self.vertices, t, vertex_id, props);
    }

    pub fn upsert_edge_props(&mut self, t: i64, edge_id: usize, props: &Vec<(String, Prop)>) {
        Self::assert_valid_edge_id(edge_id);
        Self::upsert_entity_props(&mut self.prop_ids, &mut self.edges, t, edge_id, props);
    }

    fn set_entity_meta(id_dict: &mut HashMap<String, usize>, props_storage: &mut Vec<EntityProps>, id: usize, props: &Vec<(String, Prop)>) {
        let entity_slot = Self::get_entity_slot(props_storage, id);
        for (name, prop) in props {
            let prop_id = Self::get_prop_id(id_dict, name);
            entity_slot.meta.set(prop_id, prop);
        }
    }

    pub fn set_vertex_meta(&mut self, vertex_id: usize, props: &Vec<(String, Prop)>) {
        Self::set_entity_meta(&mut self.meta_ids, &mut self.edges, vertex_id, props);
    }

    pub fn set_edge_meta(&mut self, edge_id: usize, props: &Vec<(String, Prop)>) {
        Self::assert_valid_edge_id(edge_id);
        Self::set_entity_meta(&mut self.meta_ids,&mut self.edges, edge_id, props);
    }

    fn assert_valid_edge_id(edge_id: usize) {
        if edge_id == 0 {
            panic!("Edge id (= 0) in invalid because it cannot be used to express both remote and local edges")
        };
    }
}

#[cfg(test)]
mod props_tests {
    use super::*;

    #[test]
    fn zero_index_of_edge_meta_is_preassgined_default_value() {
        let Props {
            meta_ids: _,
            prop_ids: _,
            vertices: _,
            edges: edge_meta,
        } = Props::default();

        assert_eq!(edge_meta, vec![EntityProps::empty()]);
    }

    #[test]
    fn return_valid_next_available_edge_id() {
        let props = Props::default();

        // 0th index is not a valid edge id because it can't be used to correctly denote
        // both local as well as remote edge id. Hence edge ids must always start with 1.
        assert_ne!(props.get_next_available_edge_id(), 0);
        assert_eq!(props.get_next_available_edge_id(), 1);
    }

    #[test]
    #[should_panic]
    fn assigning_edge_id_as_0_should_fail() {
        let mut props = Props::default();
        props.upsert_edge_props(1, 0, &vec![]);
    }

    #[test]
    fn return_prop_id_if_prop_name_found() {
        let mut props = Props::default();
        props.prop_ids.insert(String::from("key1"), 0);
        props.prop_ids.insert(String::from("key2"), 1);

        assert_eq!(Props::get_prop_id(&mut props.prop_ids, "key2"), 1);
    }

    #[test]
    fn return_new_prop_id_if_prop_name_not_found() {
        let mut props = Props::default();
        assert_eq!(Props::get_prop_id(&mut props.prop_ids, "key1"), 0);
        assert_eq!(Props::get_prop_id(&mut props.prop_ids, "key2"), 1);
    }

    #[test]
    fn insert_default_value_against_no_props_vertex_upsert() {
        let mut props = Props::default();
        props.upsert_vertex_props(1, 0, &vec![]);

        assert_eq!(props.vertices.get(0).unwrap().tprops, TPropVec::Empty)
    }

    #[test]
    fn insert_new_vertex_prop() {
        let mut props = Props::default();
        props.upsert_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = Props::get_prop_id(&mut props.prop_ids, "bla");
        assert_eq!(
            props
                .vertices
                .get(0)
                .unwrap()
                .tprops
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_vertex_prop() {
        let mut props = Props::default();
        props.upsert_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_vertex_props(2, 0, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = Props::get_prop_id(&mut props.prop_ids, "bla");
        assert_eq!(
            props
                .vertices
                .get(0)
                .unwrap()
                .tprops
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10)), (&2, Prop::I32(10))]
        )
    }

    #[test]
    fn new_update_with_the_same_time_to_a_vertex_prop_is_ignored() {
        let mut props = Props::default();
        props.upsert_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(20))]);

        let prop_id = Props::get_prop_id(&mut props.prop_ids, "bla");
        assert_eq!(
            props
                .vertices
                .get(0)
                .unwrap()
                .tprops
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn insert_default_value_against_no_props_edge_upsert() {
        let mut props = Props::default();
        props.upsert_edge_props(1, 1, &vec![]);

        assert_eq!(props.edges.get(1).unwrap().tprops, TPropVec::Empty)
    }

    #[test]
    fn insert_new_edge_prop() {
        let mut props = Props::default();
        props.upsert_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = Props::get_prop_id(&mut props.prop_ids, "bla");
        assert_eq!(
            props
                .edges
                .get(1)
                .unwrap()
                .tprops
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_edge_prop() {
        let mut props = Props::default();
        props.upsert_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_edge_props(2, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = Props::get_prop_id(&mut props.prop_ids, "bla");
        assert_eq!(
            props
                .edges
                .get(1)
                .unwrap()
                .tprops
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10)), (&2, Prop::I32(10))]
        )
    }

    #[test]
    fn new_update_with_the_same_time_to_a_edge_prop_is_ignored() {
        let mut props = Props::default();
        props.upsert_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(20))]);

        let prop_id = Props::get_prop_id(&mut props.prop_ids, "bla");
        assert_eq!(
            props
                .edges
                .get(1)
                .unwrap()
                .tprops
                .iter(prop_id)
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }
}
