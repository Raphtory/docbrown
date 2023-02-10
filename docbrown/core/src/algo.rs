use crate::graphview::{GraphError, GraphView, VertexViewIteratorMethods};
use crate::state::StateVec;
use itertools::{izip, Itertools};
use polars::prelude::*;
use std::cmp::min;
use std::iter::zip;

pub fn connected_components<'a>(g: &'a GraphView) -> Result<GraphView<'a>, GraphError> {
    println!("starting");
    let mut labels = g.new_state_from(g.vertices().id())?;

    for it in 0..g.n_nodes() {
        println!("next iteration {}", it);
        let new_in_labels: StateVec<u64> = g
            .vertices()
            .out_neighbours()
            .with_state(&labels)
            .map(|inner| inner.min().unwrap_or(u64::MAX))
            .collect();
        println!("{:?}", new_in_labels.values);
        let new_out_labels: StateVec<u64> = g
            .vertices()
            .in_neighbours()
            .with_state(&labels)
            .map(|inner| inner.min().unwrap_or(u64::MAX))
            .collect();
        println!("{:?}", new_out_labels.values);
        let new_labels: StateVec<u64> =
            izip!(new_in_labels.iter(), new_out_labels.iter(), labels.iter())
                .map(|(v1, v2, v3)| min(min(*v1, *v2), *v3))
                .collect();
        println!("{:?}", new_labels.values);
        let converged = labels.iter().eq(new_labels.iter());
        labels = new_labels;
        if converged {
            break;
        }
    }
    Ok(g.with_state("cc_label", labels.iter().collect()))
}

#[cfg(test)]
mod algo_tests {
    use super::*;
    use crate::graph::TemporalGraph;
    use crate::graphview::{LocalVertexView, VertexViewIteratorMethods};

    #[test]
    fn cc_test() {
        println!("very start");
        let mut g = TemporalGraph::default();

        g.add_edge(2, 1, 0);
        g.add_vertex(3, 0);
        println!("creating view");
        let gv = GraphView::new(&g, &(0..1));
        println!("we have view");
        println!("{:?}", gv.vertices().id().collect_vec());
        let gv = connected_components(&gv).unwrap();
        let cc = gv.get_state("cc_label");
        for c in cc.u64().unwrap() {
            println!("{}", c.unwrap())
        }
        for v in gv.vertices() {
            match (&v).id() {
                1 => assert_eq!(v.get_state("cc_label").extract(), Some(1)),
                2 => assert_eq!(v.get_state("cc_label").extract(), Some(1)),
                3 => assert_eq!(v.get_state("cc_label").extract(), Some(3)),
                id => panic!("unknown node {id}"),
            }
        }
    }
}
