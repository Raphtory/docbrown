use crate::graph::Graph;
use rand::seq::SliceRandom;

pub fn random_attachment(graph:&Graph, vertices_to_add:usize,edges_per_step:usize) {
    use rand::seq::IteratorRandom;
    let mut rng = &mut rand::thread_rng();
    let mut latest_time = graph.latest_time();
    let mut ids:Vec<u64> = graph.vertex_ids_window(i64::MIN,i64::MAX).collect();
    let mut max_id = match ids.iter().max() {
        Some(id) => {*id},
        None=>0
    };

    while ids.len() < edges_per_step {
        max_id+=1;
        latest_time+=1;
        graph.add_vertex(latest_time,max_id,&vec![]);
        ids.push(max_id);
    }

    for i in 0..vertices_to_add {
        let edges = ids.choose_multiple(rng,edges_per_step);
        max_id+=1;
        latest_time+=1;
        edges.for_each(|neighbour| {
            graph.add_edge(latest_time, max_id, *neighbour, &vec![]);
        });
        ids.push(max_id);
    }

}


#[cfg(test)]
mod random_graph_test {
    use super::*;
    #[test]
    fn graph_size() {
        let graph = Graph::new(2);
        random_attachment(&graph, 10,1);
        assert_eq!(graph.edges_len(), 10);
    }
}
