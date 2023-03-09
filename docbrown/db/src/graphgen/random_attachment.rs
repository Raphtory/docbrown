use crate::graph::Graph;
use rand::seq::SliceRandom;

/// This function is a graph generation model based upon:
/// Callaway, Duncan S., et al. "Are randomly grown graphs really random?." Physical Review E 64.4 (2001): 041902.
///
/// Given a graph this function will add a user defined number of vertices, each with a user defined number of edges.
/// This is an iterative algorithm where at each `step` a vertex is added and its neighbours are chosen from the pool of nodes already within the network.
/// For this model the neighbours are chosen purely at random. This sampling is done without replacement.
///
/// **Note:**  If the provided graph doesnt have enough nodes/edges for the initial sample, the min number of both will be added before generation begins.
///
/// # Arguments
/// * `graph` - The graph you wish to add vertices and edges to
/// * `vertices_to_add` - The amount of vertices you wish to add to the graph (steps)
/// * `edges_per_step` - The amount of edges a joining vertex should add to the graph
/// # Examples
///
/// ```
/// use docbrown_db::graphgen::preferential_attachment::ba_preferential_attachment;
/// use docbrown_db::graph::Graph;
/// let graph = Graph::new(2);
//  ba_preferential_attachment(&graph, 1000, 10);
/// ```
pub fn random_attachment(graph:&Graph, vertices_to_add:usize,edges_per_step:usize) {
    use rand::seq::IteratorRandom;
    let mut rng = &mut rand::thread_rng();
    let mut latest_time = match graph.latest_time() {
        None => {0}
        Some(time) => {time}
    };
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

    for _ in 0..vertices_to_add {
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
    use crate::graphgen::preferential_attachment::ba_preferential_attachment;
    use super::*;
    #[test]
    fn blank_graph() {
        let graph = Graph::new(2);
        random_attachment(&graph, 100,20);
        assert_eq!(graph.edges_len(), 2000);
        assert_eq!(graph.len(), 120);
    }

    #[test]
    fn only_nodes() {
        let graph = Graph::new(2);
        for i in 0..10{
            graph.add_vertex(i,i as u64,&vec![]);
        }

        random_attachment(&graph,1000,5);
        let window = graph.window(i64::MIN,i64::MAX);
        let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        assert_eq!(graph.edges_len(), 5000);
        assert_eq!(graph.len(),1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new(2);
        ba_preferential_attachment(&graph, 300, 7);
        random_attachment(&graph,4000,12);
        let window = graph.window(i64::MIN,i64::MAX);
        let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        assert_eq!(graph.edges_len(), 50106);
        assert_eq!(graph.len(),4307);
    }
}
