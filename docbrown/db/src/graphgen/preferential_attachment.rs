use crate::graph::Graph;
use rand::prelude::*;
use docbrown_core::Direction;
use std::collections::HashSet;

pub fn preferential_attachment(graph:&Graph,vertices_to_add:usize,edges_per_step:usize){

    let mut rng = rand::thread_rng();
    let mut latest_time = match graph.latest_time() {
        None => {0}
        Some(time) => {time}
    };
    let view = graph.window(i64::MIN,i64::MAX);
    let mut ids:Vec<u64> = view.vertex_ids().collect();
    let mut degrees:Vec<usize> = view.vertices().map(|v| v.degree()).collect();
    let mut edge_count:usize = degrees.iter().sum();

    let mut max_id = match ids.iter().max() {
        Some(id) => {*id},
        None=>0
    };

    while ids.len() < edges_per_step {
        max_id+=1;
        graph.add_vertex(latest_time,max_id,&vec![]);
        degrees.push(0);
        ids.push(max_id);
    }

    if(graph.edges_len()<edges_per_step) {
        for pos in 1..ids.len() {
            graph.add_edge(latest_time,ids[pos],ids[pos-1],&vec![]);
            edge_count+=2;
            degrees[pos]+=1;
            degrees[pos-1]+=1;
        }
    }

    for i in 0..vertices_to_add {
        max_id += 1;
        latest_time += 1;
        let mut normalisation = edge_count.clone();
        let mut positions_to_skip: HashSet<usize> = HashSet::new();

        for j in 0..edges_per_step {
            let mut sum = 0;
            let rand_num = rng.gen_range(1..=normalisation);
            for pos in 0..ids.len() {
                if ! positions_to_skip.contains(&pos){
                    sum += degrees[pos];
                    if sum>= rand_num {
                        positions_to_skip.insert(pos);
                        normalisation-=degrees[pos];
                        break;
                    }
                }
            }
        }
        for pos in positions_to_skip {
            let dst = ids[pos];
            degrees[pos]+=1;
            graph.add_edge(latest_time,max_id,dst,&vec![]);
        }
        ids.push(max_id);
        degrees.push(edges_per_step.clone());
        edge_count+=edges_per_step*2;
    }

}


//TODO need to benchmark the creation of these networks
#[cfg(test)]
mod preferential_attachment_tests {
    use crate::graphgen::random_attachment::random_attachment;
    use super::*;
    #[test]
    fn blank_graph() {
        let graph = Graph::new(2);
        preferential_attachment(&graph,1000,10);
        let window = graph.window(i64::MIN,i64::MAX);
                let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        assert_eq!(graph.edges_len(), 10009);
        assert_eq!(graph.len(),1010);
    }

    #[test]
    fn only_nodes() {
        let graph = Graph::new(2);
        for i in 0..10{
            graph.add_vertex(i,i as u64,&vec![]);
        }

        preferential_attachment(&graph,1000,5);
        let window = graph.window(i64::MIN,i64::MAX);
        let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        assert_eq!(graph.edges_len(), 5009);
        assert_eq!(graph.len(),1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new(2);
        random_attachment(&graph,1000,3);
        preferential_attachment(&graph,500,4);
        let window = graph.window(i64::MIN,i64::MAX);
        let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        assert_eq!(graph.edges_len(), 5000);
        assert_eq!(graph.len(),1503);
    }

}