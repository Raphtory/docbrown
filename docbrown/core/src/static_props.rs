use serde::{Deserialize, Serialize};

use crate::Prop;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) enum StaticPropVec {
    Empty,
    // First tuple value in "MetaVec1" and indices in "MetaVecN" vector denote property id
    // values from "Props::meta_ids" hashmap
    StaticPropVec1(usize, Prop), // is this optimization really worth it, shouldnt be just like empty or vector?
    StaticPropVecN(Vec<Option<Prop>>), // Option<Prop> has the same size as Prop
}

impl StaticPropVec {
    pub(crate) fn from_static(prop_id: usize, prop: &Prop) -> Self {
        StaticPropVec::StaticPropVec1(prop_id, prop.clone().into())
    }

    pub(crate) fn set_static(&mut self, prop_id: usize, prop: &Prop) {
        match self {
            StaticPropVec::Empty => {
                *self = StaticPropVec::from_static(prop_id, prop);
            }
            StaticPropVec::StaticPropVec1(prop_id0, prop0) => {
                if *prop_id0 == prop_id {
                    Self::handle_illegal_change(prop_id);
                } else {
                    let mut props = vec![None; usize::max(prop_id, *prop_id0) + 1];
                    props[prop_id] = Some(prop.clone());
                    props[*prop_id0] = Some(prop0.clone());
                    *self = StaticPropVec::StaticPropVecN(props);
                }
            }
            StaticPropVec::StaticPropVecN(props) => {
                if props.len() <= prop_id {
                    props.resize(prop_id + 1, None)
                }
                if props[prop_id] == None {
                    props[prop_id] = Some(prop.clone());
                } else {
                    Self::handle_illegal_change(prop_id);
                }
            }
        }
    }

    pub(crate) fn get_static(&self, id: usize) -> Option<Prop> {
        match self {
            StaticPropVec::Empty => None,
            StaticPropVec::StaticPropVec1(only_id, prop) => {
                if *only_id == id {
                    Some(prop.clone().into())
                } else {
                    None
                }
            },
            StaticPropVec::StaticPropVecN(metavec) => {
                match metavec.get(id) {
                    Some(prop) => prop.clone(),
                    None => None
                }
            }
        }
    }

    fn handle_illegal_change(id: usize) {
        panic!("Tried to change meta data for id '{}'", id); // TODO: handle this properly
    }
}