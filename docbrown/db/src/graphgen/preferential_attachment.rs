use crate::graph::Graph;
use rand::prelude::*;
use docbrown_core::Direction;
use std::collections::HashSet;

//let mut weights:Vec<usize> =view.vertices().map(|v|v.degree()).collect();

pub fn preferential_attachment(graph:&Graph,vertices_to_add:usize,edges_per_step:usize){

    let mut rng = rand::thread_rng();
    let mut latest_time = graph.latest_time();

    let view = graph.window(i64::MIN,i64::MAX);
    let mut ids:Vec<u64> = view.vertex_ids().collect();
    let mut degrees:Vec<usize> = view.vertices().map(|v| v.degree()).collect();
    let mut edge_count:usize = degrees.iter().sum();

    let mut max_id = match ids.iter().max() {
        Some(id) => {*id},
        None=>0
    };

    if(ids.len()==0){
        graph.add_vertex(latest_time,0,&vec![]);
        degrees.push(0);
        ids.push(0);
    }
    while ids.len() < edges_per_step {
        max_id+=1;
        latest_time+=1;
        let prev_node = ids[ids.len()-1];
        graph.add_vertex(latest_time,max_id,&vec![]);
        graph.add_edge(latest_time,max_id,prev_node,&vec![]);
        degrees[ids.len()-1]+=1;
        degrees.push(1);
        ids.push(max_id);
        edge_count+=2;

    }


    for i in 0..vertices_to_add {
        max_id += 1;
        latest_time += 1;
        let mut normalisation = edge_count.clone();
        let mut positions_to_skip: HashSet<usize> = HashSet::new();

        for j in 0..edges_per_step {
            let mut sum = 0;
            let rand_num = rng.gen_range(1..=normalisation);
            for(pos,id) in ids.iter().enumerate() {
                if ! positions_to_skip.contains(&pos){
                    sum += degrees[pos];
                    if sum>= rand_num {
                        positions_to_skip.insert(pos);
                        normalisation-degrees[pos];
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


//TODO need to add a known seed and test that a correct distribution is generated
//TODO need to benchmark the creation of these networks
#[cfg(test)]
mod preferential_attachment_tests {
    use super::*;
    #[test]
    fn graph_size() {
        let graph = Graph::new(2);
        preferential_attachment(&graph,1000,10);
        let window = graph.window(i64::MIN,i64::MAX);
                let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        degree.sort();
        println!("{:?}",degree);
        //TODO this fails for sure
        assert_eq!(graph.edges_len(), 1010);
    }
}