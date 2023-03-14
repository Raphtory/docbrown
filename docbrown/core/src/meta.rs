use serde::{Deserialize, Serialize};

use crate::Prop;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) enum MetaVec {
    Empty,
    // First tuple value in "MetaVec1" and indices in "MetaVecN" vector denote property id
    // values from "Props::meta_ids" hashmap
    MetaVec1(usize, Prop), // is this optimization really worth it, shouldnt be just like empty or vector?
    MetaVecN(Vec<Option<Prop>>), // todo why not a hashmap here? because there are few properties normally
}

impl MetaVec {
    pub(crate) fn from(prop_id: usize, prop: &Prop) -> Self {
        MetaVec::MetaVec1(prop_id, prop.clone().into())
    }

    pub(crate) fn set(&mut self, prop_id: usize, prop: &Prop) {
        match self {
            MetaVec::Empty => {
                *self = MetaVec::from(prop_id, prop);
            }
            MetaVec::MetaVec1(prop_id0, prop0) => {
                if *prop_id0 == prop_id {
                    Self::handle_illegal_change(prop_id);
                } else {
                    let mut props = vec![None; usize::max(prop_id, *prop_id0) + 1];
                    props[prop_id] = Some(prop.clone());
                    props[*prop_id0] = Some(prop0.clone());
                    *self = MetaVec::MetaVecN(props);
                }
            }
            MetaVec::MetaVecN(props) => {
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

    pub(crate) fn get(&self, id: usize) -> Option<Prop> {
        match self {
            MetaVec::Empty => None,
            MetaVec::MetaVec1(only_id, prop) => {
                if *only_id == id {
                    Some(prop.clone().into())
                } else {
                    None
                }
            },
            MetaVec::MetaVecN(metavec) => {
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