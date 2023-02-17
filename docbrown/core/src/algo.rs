use crate::error::GraphError;
use crate::graphview::{GraphView, StateView};
use crate::state::{State, StateVec};
use crate::vertexview::{VertexViewMethods, VertexViewStateMethods};
use itertools::izip;
use std::cmp::min;

pub fn connected_components<G>(g: G) -> Result<G, GraphError>
where
    G: GraphView + StateView,
{
    println!("starting");
    let mut labels = g.new_state_from(g.vertices().id())?;

    for it in 0..g.n_vertices() {
        println!("next iteration {}", it);
        let new_in_labels: G::StateType<u64> = g.new_state_from(
            g.vertices()
                .out_neighbours()
                .with_state(&labels)
                .map(|inner| inner.min().unwrap_or(u64::MAX)),
        )?;
        let new_out_labels: G::StateType<u64> = g.new_state_from(
            g.vertices()
                .in_neighbours()
                .with_state(&labels)
                .map(|inner| inner.min().unwrap_or(u64::MAX)),
        )?;
        let new_labels = g.new_state_from(
            izip!(new_in_labels.iter(), new_out_labels.iter(), labels.iter())
                .map(|(v1, v2, v3)| min(min(*v1, *v2), *v3)),
        )?;
        let converged = labels.iter().eq(new_labels.iter());
        labels = new_labels;
        if converged {
            break;
        }
    }
    g.with_state("cc_label", labels.iter().collect())
}

#[cfg(test)]
mod algo_tests {
    use super::*;
    use crate::graphview::{MutableGraph, WindowedView};
    use crate::singlepartitiongraph::SinglePartitionGraph;
    use crate::vertexview::VertexViewMethods;
    use itertools::Itertools;

    #[test]
    fn cc_test() {
        println!("very start");
        let mut g = SinglePartitionGraph::default();

        g.add_edge(2, 1, 0);
        g.add_vertex(3, 0);
        println!("creating view");
        let gv = WindowedView::new(&g, 0..1);
        println!("we have view");
        println!("{:?}", gv.vertices().id().collect_vec());
        let gv = connected_components(gv).unwrap();
        let cc = gv.get_state("cc_label").unwrap();
        for c in cc.u64().unwrap() {
            println!("{}", c.unwrap())
        }

        for v in gv.vertices() {
            match (v.clone()).id() {
                1 => assert_eq!(v.get_property("cc_label").unwrap().extract(), Some(1)),
                2 => assert_eq!(v.get_property("cc_label").unwrap().extract(), Some(1)),
                3 => assert_eq!(v.get_property("cc_label").unwrap().extract(), Some(3)),
                id => panic!("unknown node {id}"),
            }
        }
    }
}
