



#[cfg(test)]
mod algo_tests {
    use super::*;
    use crate::graph::TemporalGraph;

    #[test]
    fn cc_test() {
        let mut g = TemporalGraph::default();

        g.add_edge(1, 2, 0);
        for v in g.iter_vertices() {
            println!("{}", v.global_id())
        }

    }
}