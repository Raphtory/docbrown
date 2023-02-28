use docbrown_db::graph::Graph;
use std::error::Error;
use docbrown_core::Direction;
use itertools::Itertools;


pub fn local_triangle_count(g: &Graph, v: u64) -> u32
{
    let mut number_of_triangles: u32 = 0;
    // If graph contains vertex and it has degree more than or equal to 2
    if g.contains(v) && g.degree(v, Direction::BOTH) >= 2  {
        //Iterate through each neighbour of vertex (1)
        g.vertex_ids().combinations(2)
        .for_each(|v| {
            if g.has_edge(v[0],v[1]) || (g.has_edge(v[1],v[0]) ){
                number_of_triangles+=1;
            }      
        });
        }

    number_of_triangles

}
