use std::cmp::min;
use polars::prelude::*;
use crate::graphview::GraphView;


pub fn connected_components<'a>(g: &'a GraphView) -> GraphView<'a> {
    println!("starting");
    let mut g = g.with_state("cc_label", g.ids());
    for it in 0..g.n_nodes() {
        println!("next iteration {}", it);
        let old_state = g.get_state("cc_label");
        let new_state: Series = g.iter_vertices().map(|v| {
            let in_min = v.in_neighbours().map(|v| {
                let value: u64 = v.get_state("cc_label").extract().unwrap();
                value
            }).min().unwrap_or(v.global_id());
            let out_min = v.out_neighbours().map(|v| {
                let value: u64 = v.get_state("cc_label").extract().unwrap();
                value
            }).min().unwrap_or(v.global_id());
            min(min(in_min, out_min), v.global_id())
        }).collect();
        let changed = old_state.u64().unwrap().not_equal(new_state.u64().unwrap()).any();
        if changed {
            println!("not converged");
            g = g.with_state("cc_label", new_state);
        } else {
            println!("converged");
            break
        }
    }
    g
}



#[cfg(test)]
mod algo_tests {
    use super::*;
    use crate::graph::TemporalGraph;

    #[test]
    fn cc_test() {
        println!("very start");
        let mut g = TemporalGraph::default();

        g.add_edge(2, 1, 0);
        g.add_vertex(3, 0);
        println!("creating view");
        let gv = GraphView::new(&g, &(0..1));
        println!("we have view");
        let gv = connected_components(&gv);
        let cc = gv.get_state("cc_label");
        for c in cc.u64().unwrap() {
            println!("{}", c.unwrap())
        }


    }
}