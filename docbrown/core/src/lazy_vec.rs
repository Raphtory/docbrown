use crate::tprop::TProp;
use crate::Prop;
use serde::{Deserialize, Serialize};
use std::ops::Range;


#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum LazyVec<A> {
    #[default]
    Empty,
    // First tuple value in "TPropVec1" and indices in "TPropVecN" vector denote property id
    // values from "Props::prop_ids" hashmap
    LazyVec1(usize, A),
    LazyVecN(Vec<A>),
}

impl<A> LazyVec<A>
where
    A: PartialEq + Default + Clone
{
    pub(crate) fn from(id: usize, value: A) -> Self {
        LazyVec::LazyVec1(id, value)
    }

    pub(crate) fn filled_ids(&self) -> Vec<usize> {
        match self {
            LazyVec::Empty => Default::default(),
            LazyVec::LazyVec1(id, _) => vec![*id],
            LazyVec::LazyVecN(vector) => {
                vector.iter().enumerate()
                    .filter(|&(id, value)| *value != Default::default())
                    .map(|(id, _)| id)
                    .collect()
            }
        }
    }

    pub(crate) fn get_mut(&mut self, id: usize) -> Option<&mut A> {
        match self {
            LazyVec::LazyVec1(only_id, value) if *only_id == id => Some(value),
            LazyVec::LazyVecN(vec) => vec.get_mut(id),
            _ => None,
        }
    }

    pub(crate) fn get(&self, id: usize) -> Option<&A> {
        match self {
            LazyVec::LazyVec1(only_id, value) if *only_id == id => Some(value),
            LazyVec::LazyVecN(vec) => vec.get(id),
            _ => None,
        }
    }

    // fails if there is already a value set for the given id and it's different from the default
    pub(crate) fn set(&mut self, id: usize, value: A) -> Result<(), ()> {
        match self {
            LazyVec::Empty => {
                *self = Self::from(id, value);
                Ok(())
            }
            LazyVec::LazyVec1(only_id, only_value) => {
                if *only_id == id {
                    Err(())
                } else {
                    let mut vector = vec![Default::default(); usize::max(id, *only_id) + 1];
                    vector[id] = value;
                    vector[*only_id] = only_value.clone();
                    *self = LazyVec::LazyVecN(vector);
                    Ok(())
                }
            }
            LazyVec::LazyVecN(vector) => {
                if vector.len() <= id {
                    vector.resize(id + 1, Default::default())
                }
                if vector[id] == Default::default() {
                    vector[id] = value;
                    Ok(())
                } else {
                    Err(())
                }
            }
        }
    }

    pub(crate) fn update_or_set<F>(&mut self, id: usize, mut updater: F, default: A)
    where
        F: FnMut(&mut A) // TODO: is FnMut right?
    {
        match self.get_mut(id) {
            Some(value) => updater(value),
            None => self.set(id, default).unwrap(),
        }
    }



    fn handle_illegal_change(id: usize) {
        panic!("Tried to change meta data for id '{}'", id); // TODO: move this somewhere else and use it for static props
    }
}

// TODO: move all these test to Props ?

// #[cfg(test)]
// mod tpropvec_tests {
//     use super::*;
//
//     #[test]
//     fn set_new_prop_for_tpropvec_initialized_as_empty() {
//         let mut tpropvec = LazyVec::Empty;
//         let prop_id = 1;
//         tpropvec.set_temporal(prop_id, 1, &Prop::I32(10));
//
//         assert_eq!(
//             tpropvec.iter_temporal(prop_id).collect::<Vec<_>>(),
//             vec![(&1, Prop::I32(10))]
//         );
//     }
//
//     #[test]
//     fn set_multiple_props() {
//         let mut tpropvec = LazyVec::from_temporal(1, 1, &Prop::Str("Pometry".into()));
//         tpropvec.set_temporal(2, 2, &Prop::I32(2022));
//         tpropvec.set_temporal(3, 3, &Prop::Str("Graph".into()));
//
//         assert_eq!(
//             tpropvec.iter_temporal(1).collect::<Vec<_>>(),
//             vec![(&1, Prop::Str("Pometry".into()))]
//         );
//         assert_eq!(
//             tpropvec.iter_temporal(2).collect::<Vec<_>>(),
//             vec![(&2, Prop::I32(2022))]
//         );
//         assert_eq!(
//             tpropvec.iter_temporal(3).collect::<Vec<_>>(),
//             vec![(&3, Prop::Str("Graph".into()))]
//         );
//     }
//
//     #[test]
//     fn every_new_update_to_the_same_prop_is_recorded_as_history() {
//         let mut tpropvec = LazyVec::from_temporal(1, 1, &Prop::Str("Pometry".into()));
//         tpropvec.set_temporal(1, 2, &Prop::Str("Pometry Inc.".into()));
//
//         let prop1 = tpropvec.iter_temporal(1).collect::<Vec<_>>();
//         assert_eq!(
//             prop1,
//             vec![
//                 (&1, Prop::Str("Pometry".into())),
//                 (&2, Prop::Str("Pometry Inc.".into()))
//             ]
//         );
//     }
//
//     #[test]
//     fn new_update_with_the_same_time_to_a_prop_is_ignored() {
//         let mut tpropvec = LazyVec::from_temporal(1, 1, &Prop::Str("Pometry".into()));
//         tpropvec.set_temporal(1, 1, &Prop::Str("Pometry Inc.".into()));
//
//         let prop1 = tpropvec.iter_temporal(1).collect::<Vec<_>>();
//         assert_eq!(prop1, vec![(&1, Prop::Str("Pometry".into()))]);
//     }
//
//     #[test]
//     fn updates_to_every_prop_can_be_iterated() {
//         let tpropvec = LazyVec::default();
//
//         assert_eq!(tpropvec.iter_temporal(1).collect::<Vec<_>>(), vec![]);
//
//         let mut tpropvec = LazyVec::from_temporal(1, 1, &Prop::Str("Pometry".into()));
//         tpropvec.set_temporal(1, 2, &Prop::Str("Pometry Inc.".into()));
//         tpropvec.set_temporal(2, 3, &Prop::I32(2022));
//         tpropvec.set_temporal(3, 4, &Prop::Str("Graph".into()));
//         tpropvec.set_temporal(3, 5, &Prop::Str("Graph Analytics".into()));
//
//         let prop1 = tpropvec.iter_temporal(1).collect::<Vec<_>>();
//         assert_eq!(
//             prop1,
//             vec![
//                 (&1, Prop::Str("Pometry".into())),
//                 (&2, Prop::Str("Pometry Inc.".into()))
//             ]
//         );
//
//         let prop3 = tpropvec.iter_temporal(3).collect::<Vec<_>>();
//         assert_eq!(
//             prop3,
//             vec![
//                 (&4, Prop::Str("Graph".into())),
//                 (&5, Prop::Str("Graph Analytics".into()))
//             ]
//         );
//     }
//
//     #[test]
//     fn updates_to_every_prop_can_be_window_iterated() {
//         let tpropvec = LazyVec::default();
//
//         assert_eq!(
//             tpropvec
//                 .iter_window(1, i64::MIN..i64::MAX)
//                 .collect::<Vec<_>>(),
//             vec![]
//         );
//
//         let mut tpropvec = LazyVec::from_temporal(1, 1, &Prop::Str("Pometry".into()));
//         tpropvec.set_temporal(1, 2, &Prop::Str("Pometry Inc.".into()));
//         tpropvec.set_temporal(2, 3, &Prop::I32(2022));
//         tpropvec.set_temporal(3, 4, &Prop::Str("Graph".into()));
//         tpropvec.set_temporal(3, 5, &Prop::Str("Graph Analytics".into()));
//         tpropvec.set_temporal(2, 1, &Prop::I32(2021));
//         tpropvec.set_temporal(2, 4, &Prop::I32(2023));
//
//         let prop1 = tpropvec.iter_window(1, 1..3).collect::<Vec<_>>();
//         assert_eq!(
//             prop1,
//             vec![
//                 (&1, Prop::Str("Pometry".into())),
//                 (&2, Prop::Str("Pometry Inc.".into()))
//             ]
//         );
//
//         let prop3 = tpropvec.iter_window(3, 5..6).collect::<Vec<_>>();
//         assert_eq!(prop3, vec![(&5, Prop::Str("Graph Analytics".into()))]);
//
//         assert_eq!(tpropvec.iter_window(2, 5..6).collect::<Vec<_>>(), vec![]);
//
//         assert_eq!(
//             // Results are ordered by time
//             tpropvec.iter_window(2, 1..i64::MAX).collect::<Vec<_>>(),
//             vec![
//                 (&1, Prop::I32(2021)),
//                 (&3, Prop::I32(2022)),
//                 (&4, Prop::I32(2023))
//             ]
//         );
//
//         assert_eq!(
//             tpropvec.iter_window(2, 4..i64::MAX).collect::<Vec<_>>(),
//             vec![(&4, Prop::I32(2023))]
//         );
//
//         assert_eq!(
//             tpropvec.iter_window(2, 2..i64::MAX).collect::<Vec<_>>(),
//             vec![(&3, Prop::I32(2022)), (&4, Prop::I32(2023))]
//         );
//
//         assert_eq!(
//             tpropvec.iter_window(2, 5..i64::MAX).collect::<Vec<_>>(),
//             vec![]
//         );
//
//         assert_eq!(
//             tpropvec.iter_window(2, i64::MIN..5).collect::<Vec<_>>(),
//             // Results are ordered by time
//             vec![
//                 (&1, Prop::I32(2021)),
//                 (&3, Prop::I32(2022)),
//                 (&4, Prop::I32(2023))
//             ]
//         );
//
//         assert_eq!(
//             tpropvec.iter_window(2, i64::MIN..1).collect::<Vec<_>>(),
//             vec![]
//         );
//     }
// }
