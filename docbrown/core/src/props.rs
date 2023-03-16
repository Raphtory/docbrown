use crate::lazy_vec::LazyVec;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use itertools::Itertools;
use crate::tprop::TProp;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
enum PropId {
    Static(usize),
    Temporal(usize),
}

impl PropId {
    pub(crate) fn new(id: usize, static_: bool) -> PropId {
        if static_ {
            PropId::Static(id)
        } else {
            PropId::Temporal(id)
        }
    }
    pub(crate) fn get_id(&self) -> usize {
        match self {
            PropId::Static(id) => *id,
            PropId::Temporal(id) => *id,
        }
    }
    pub(crate) fn is_static(&self) -> bool {
        match self {
            PropId::Static(_) => true,
            PropId::Temporal(_) => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Props {
    // Mapping between property name and property id
    prop_ids: HashMap<String, PropId>, // TODO: change name back to prop_ids
    reverse_ids: HashMap<PropId, String>,

    // Vector of vertices properties. Each index represents vertex local (physical) id
    static_vertex_props: Vec<LazyVec<Option<Prop>>>,
    temporal_vertex_props: Vec<LazyVec<TProp>>,
    num_vertex_slots: usize,

    // Vector of edge properties. Each "signed" index represents an edge id
    static_edge_props: Vec<LazyVec<Option<Prop>>>,
    temporal_edge_props: Vec<LazyVec<TProp>>,
    num_edge_slots: usize,
}

impl Default for Props {
    fn default() -> Self {
        Self {
            prop_ids: Default::default(),
            reverse_ids: Default::default(),
            static_vertex_props: Default::default(),
            temporal_vertex_props: Default::default(),
            num_vertex_slots: 0,
            static_edge_props: Default::default(),
            temporal_edge_props: Default::default(),
            // Edge ids refer to the position of properties inside self.props.temporal_edge_props and
            // self.props.temporal_edge_props. Besides, negative and positive indices are used to denote
            // remote and local edges, respectively. Therefore, index "0" can be used to denote neither
            // local nor remote edges, which simply breaks this symmetry
            // Hence, the first id to be provided as edge id is 1
            num_edge_slots: 1,
        }
    }
}

impl Props {
    pub(crate) fn get_next_available_edge_id(&mut self) -> usize {
        self.num_edge_slots
    }

    pub(crate) fn static_vertex_props(&self, vertex_id: usize) -> &LazyVec<Option<Prop>> {
        if vertex_id >= self.num_vertex_slots {
            panic!("vertex_id {vertex_id} outside static_vertex_props bounds");
        }
        self.static_vertex_props.get(vertex_id).unwrap_or(&LazyVec::Empty)
    }

    pub(crate) fn temporal_vertex_props(&self, vertex_id: usize) -> &LazyVec<TProp> {
        if vertex_id >= self.num_vertex_slots {
            panic!("vertex_id {vertex_id} outside temporal_vertex_props bounds");
        }
        self.temporal_vertex_props.get(vertex_id).unwrap_or(&LazyVec::Empty)
    }

    pub(crate) fn static_edge_props(&self, edge_id: usize) -> &LazyVec<Option<Prop>> {
        if edge_id >= self.num_edge_slots {
            panic!("vertex_id {edge_id} outside static_edge_props bounds");
        }
        self.static_edge_props.get(edge_id).unwrap_or(&LazyVec::Empty)
    }

    pub(crate) fn temporal_edge_props(&self, edge_id: usize) -> &LazyVec<TProp> {
        if edge_id >= self.num_edge_slots {
            panic!("vertex_id {edge_id} outside temporal_edge_props bounds");
        }
        self.temporal_edge_props.get(edge_id).unwrap_or(&LazyVec::Empty)
    }

    fn get_or_allocate_id(&mut self, name: &str, should_be_static: bool) -> Result<PropId, ()> {
        match self.prop_ids.get(name) {
            None => {
                let new_prop_id = if should_be_static {
                    let static_prop_ids = self.prop_ids.iter().filter(|&(k, v)| v.is_static());
                    let new_id = static_prop_ids.count();
                    PropId::Static(new_id)
                } else {
                    let static_prop_ids = self.prop_ids.iter().filter(|&(k, v)| !v.is_static());
                    let new_id = static_prop_ids.count();
                    PropId::Temporal(new_id)
                };
                self.prop_ids.insert(name.to_string(), new_prop_id.clone());
                // we are inserting a new key into the dictionary, so we update the reversed dictionary:
                self.reverse_ids.insert(new_prop_id.clone(), name.to_string());
                Ok(new_prop_id)
            },
            Some(id) if id.is_static() == should_be_static => Ok(id.clone()),
            _ => Err(()),
        }
    }

    fn get_or_allocate_static_id(&mut self, name: &str) -> Result<usize, ()> {
        self.get_or_allocate_id(name, true).map(|p| p.get_id())
    }

    fn get_or_allocate_temporal_id(&mut self, name: &str) -> Result<usize, ()> {
        self.get_or_allocate_id(name, false).map(|p| p.get_id())
    }

    fn get_id(&self, name: &str,  should_be_static: bool) -> Option<usize> {
        match self.prop_ids.get(name) {
            Some(prop_id) if prop_id.is_static() == should_be_static => Some(prop_id.get_id()),
            _ => None,
        }
    }

    pub(crate) fn get_static_id(&self, name: &str) -> Option<usize> {
        dbg!(&self.prop_ids);
        self.get_id(name, true)
    }

    pub(crate) fn get_temporal_id(&self, name: &str) -> Option<usize> {
        self.get_id(name, false)
    }

    fn grow_and_get_slot<A>(vector: &mut Vec<A>, id: usize) -> &mut A
    where
        A: Default
    {
        if vector.len() <= id {
            vector.resize_with(id + 1, || Default::default());
        }
        // now props_storage.len() >= id + 1:
        vector.get_mut(id).unwrap()
    }

    fn translate_props(&mut self, props: &Vec<(String, Prop)>, should_be_static: bool) -> Vec<(usize, Prop)> { // TODO: return Result
        props.iter()
            .map(|(name, prop)| (self.get_or_allocate_id(&name, should_be_static).unwrap().get_id(), prop.clone()))
            .collect_vec()
    }

    pub fn upsert_temporal_vertex_props(&mut self, t: i64, vertex_id: usize, props: &Vec<(String, Prop)>) -> Result<(), ()> {
        self.num_vertex_slots += 1;
        if !props.is_empty() {
            let translated_props = self.translate_props(props, false);
            let vertex_slot: &mut LazyVec<TProp> = Self::grow_and_get_slot(&mut self.temporal_vertex_props, vertex_id);
            for (prop_id, prop) in translated_props {
                vertex_slot.update_or_set(prop_id, |p| p.set(t, &prop), TProp::from(t, &prop));
            }
        }
        Ok(())
    }

    pub fn upsert_temporal_edge_props(&mut self, t: i64, edge_id: usize, props: &Vec<(String, Prop)>) -> Result<(), ()>  {
        self.num_edge_slots += 1;
        Self::assert_valid_edge_id(edge_id);
        if !props.is_empty() {
            let translated_props = self.translate_props(props, false);
            let edge_slot: &mut LazyVec<TProp> = Self::grow_and_get_slot(&mut self.temporal_edge_props, edge_id);
            for (prop_id, prop) in translated_props {
                edge_slot.update_or_set(prop_id, |p| p.set(t, &prop), TProp::from(t, &prop));
            }
        }
        Ok(())
    }

    pub fn set_vertex_meta(&mut self, vertex_id: usize, props: &Vec<(String, Prop)>) -> Result<(), ()> {
        if !props.is_empty() {
            let translated_props = self.translate_props(props, true);
            let vertex_slot: &mut LazyVec<Option<Prop>> = Self::grow_and_get_slot(&mut self.static_vertex_props, vertex_id);
            for (prop_id, prop) in translated_props {
                vertex_slot.set(prop_id, Some(prop));
            }
            dbg!(vertex_slot);
        }
        Ok(())
    }

    pub fn set_edge_meta(&mut self, edge_id: usize, props: &Vec<(String, Prop)>) -> Result<(), ()> {
        Self::assert_valid_edge_id(edge_id);
        if !props.is_empty() {
            let translated_props = self.translate_props(props, true);
            let edge_slot: &mut LazyVec<Option<Prop>> = Self::grow_and_get_slot(&mut self.static_edge_props, edge_id);
            for (prop_id, prop) in translated_props {
                edge_slot.set(prop_id, Some(prop));
            }
        }
        Ok(())
    }

    pub fn static_vertex_keys(&self, vertex_id: usize) -> Vec<String> {
        match self.static_vertex_props.get(vertex_id) {
            Some(tprops) => {
                let ids = tprops.filled_ids();
                ids.into_iter()
                    .map(|id| self.reverse_ids.get(&PropId::Static(id)).unwrap().to_string())
                    .collect_vec()
            },
            None => Default::default(),
        }
    }

    pub fn static_edge_keys(&self, edge_id: usize) -> Vec<String> {
        match self.static_edge_props.get(edge_id) {
            Some(tprops) => {
                let ids = tprops.filled_ids();
                ids.into_iter()
                    .map(|id| self.reverse_ids.get(&PropId::Static(id)).unwrap().to_string())
                    .collect_vec()
            },
            None => Default::default(),
        }
    }

    pub fn temporal_vertex_keys(&self, vertex_id: usize) -> Vec<String> {
        match self.temporal_vertex_props.get(vertex_id) {
            Some(tprops) => {
                let ids = tprops.filled_ids();
                ids.into_iter()
                    .map(|id| self.reverse_ids.get(&PropId::Temporal(id)).unwrap().to_string())
                    .collect_vec()
            },
            None => Default::default(),
        }
    }

    pub fn temporal_edge_keys(&self, edge_id: usize) -> Vec<String> {
        match self.temporal_edge_props.get(edge_id) {
            Some(tprops) => {
                let ids = tprops.filled_ids();
                ids.into_iter()
                    .map(|id| self.reverse_ids.get(&PropId::Temporal(id)).unwrap().to_string())
                    .collect()
            },
            None => Default::default(),
        }
    }

    fn assert_valid_edge_id(edge_id: usize) { // TODO: this should return a result
        if edge_id == 0 {
            panic!("Edge id (= 0) in invalid because it cannot be used to express both remote and local edges")
        };
    }
}

#[cfg(test)]
mod props_tests {
    use super::*;

    #[test]
    fn return_valid_next_available_edge_id() {
        let mut props = Props::default();

        // 0th index is not a valid edge id because it can't be used to correctly denote
        // both local as well as remote edge id. Hence edge ids must always start with 1.
        assert_eq!(props.get_next_available_edge_id(), 1);
    }

    #[test]
    #[should_panic]
    fn assigning_edge_id_as_0_should_fail() {
        let mut props = Props::default();
        props.upsert_temporal_edge_props(1, 0, &vec![]);
    }

    #[test]
    fn return_prop_id_if_prop_name_found() {
        let mut props = Props::default();
        props.prop_ids.insert(String::from("key1"), PropId::Temporal(0));
        props.prop_ids.insert(String::from("key2"), PropId::Temporal(1));

        assert_eq!(props.get_or_allocate_temporal_id("key2"), Ok(1));
    }

    #[test]
    fn return_new_prop_id_if_prop_name_not_found() {
        let mut props = Props::default();
        assert_eq!(props.get_or_allocate_temporal_id("key1"), Ok(0));
        assert_eq!(props.get_or_allocate_temporal_id("key2"), Ok(1));
    }

    #[test]
    fn insert_default_value_against_no_props_vertex_upsert() {
        let mut props = Props::default();
        props.upsert_temporal_vertex_props(1, 0, &vec![]);

        assert_eq!(props.temporal_vertex_props(0), &LazyVec::Empty)
    }

    #[test]
    fn insert_new_vertex_prop() {
        let mut props = Props::default();
        props.upsert_temporal_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_or_allocate_temporal_id("bla").unwrap();
        assert_eq!(
            props
                .temporal_vertex_props
                .get(0)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_vertex_prop() {
        let mut props = Props::default();
        props.upsert_temporal_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_temporal_vertex_props(2, 0, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_or_allocate_temporal_id("bla").unwrap();
        assert_eq!(
            props
                .temporal_vertex_props
                .get(0)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10)), (&2, Prop::I32(10))]
        )
    }

    #[test]
    fn new_update_with_the_same_time_to_a_vertex_prop_is_ignored() {
        let mut props = Props::default();
        props.upsert_temporal_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_temporal_vertex_props(1, 0, &vec![("bla".to_string(), Prop::I32(20))]);

        let prop_id = props.get_or_allocate_temporal_id("bla").unwrap();
        assert_eq!(
            props
                .temporal_vertex_props
                .get(0)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn insert_default_value_against_no_props_edge_upsert() {
        let mut props = Props::default();
        props.upsert_temporal_edge_props(1, 1, &vec![]);

        assert_eq!(props.temporal_edge_props(1), &LazyVec::Empty)
    }

    #[test]
    fn insert_new_edge_prop() {
        let mut props = Props::default();
        props.upsert_temporal_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_or_allocate_temporal_id("bla").unwrap();
        assert_eq!(
            props
                .temporal_edge_props(1)
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }

    #[test]
    fn update_existing_edge_prop() {
        let mut props = Props::default();
        props.upsert_temporal_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_temporal_edge_props(2, 1, &vec![("bla".to_string(), Prop::I32(10))]);

        let prop_id = props.get_or_allocate_temporal_id("bla").unwrap();
        assert_eq!(
            props
                .temporal_edge_props
                .get(1)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10)), (&2, Prop::I32(10))]
        )
    }

    #[test]
    fn new_update_with_the_same_time_to_a_edge_prop_is_ignored() {
        let mut props = Props::default();
        props.upsert_temporal_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(10))]);
        props.upsert_temporal_edge_props(1, 1, &vec![("bla".to_string(), Prop::I32(20))]);

        let prop_id = props.get_or_allocate_temporal_id("bla").unwrap();
        assert_eq!(
            props
                .temporal_edge_props
                .get(1)
                .unwrap()
                .get(prop_id)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![(&1, Prop::I32(10))]
        )
    }
}
