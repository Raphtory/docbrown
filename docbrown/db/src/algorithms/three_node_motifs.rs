use std::ops::Index;

use itertools::enumerate;

use crate::view_api::*;

const incoming: usize = 0;
const outgoing: usize = 1;
const dirs2D: [(usize,usize);4] = [(0,0),(0,1),(1,0),(1,1)];

fn map2D(d1:usize, d2:usize) -> usize {
    2*d1 + d2
}
fn map3D(d1:usize, d2:usize, d3:usize) -> usize {
    4*d1 + 2*d2 + d3
}

// Two Node Motifs
struct TwoNodeEvent{
    dir:usize,
    time:i64,
}
struct TwoNodeCounter {
    count1d:[u64;2],
    count2d:[u64;4],
    count3d:[u64;8],
}
impl TwoNodeCounter {

    fn execute(&mut self, events:&[TwoNodeEvent], delta: i64) {
        let mut start = 0;
        for event in events.iter() {
            while events[start].time + delta < event.time {
                self.decrement_counts(events[start].dir);
                start +=1;
            }
            self.increment_counts(events[start].dir);
        }
    }

    fn decrement_counts(&mut self, dir:usize) {
        self.count1d[dir]-=1;
        self.count2d[map2D(dir, incoming)] -= self.count1d[incoming];
        self.count2d[map2D(dir,outgoing)] -= self.count1d[outgoing];
    }

    fn increment_counts(&mut self, dir:usize) {
        // 3d counter
        for (d1,d2) in dirs2D {
            self.count3d[map3D(d1, d2, dir)] += self.count2d[map2D(d1, d2)];
        }
        // 2d counter
        self.count2d[map2D(incoming,dir)]+=self.count1d[incoming];
        self.count2d[map2D(outgoing, dir)]+=self.count1d[outgoing];
        // 1d counter
        self.count1d[dir]+=1;
    }
}

// Star Motifs
struct StarEvent{
    nb:usize,
    dir:usize,
    time:usize,
}
struct StarCounter{
    N: usize,
    pre_nodes:Vec<usize>,
    post_nodes:Vec<usize>,
    pre_sum:[usize;8],
    mid_sum:[usize;8],
    post_sum:[usize;8],
    count_pre:[usize;8],
    count_mid:[usize;8],
    count_post:[usize;8],
    final_counts:[usize;8],
}
impl StarCounter { 
    fn push_pre(&mut self, cur_edge:&StarEvent) {
        self.pre_sum[map2D(incoming, cur_edge.dir)] += self.pre_nodes[incoming*self.N + cur_edge.nb];
        self.pre_sum[map2D(outgoing, cur_edge.dir)] += self.pre_nodes[outgoing*self.N + cur_edge.nb];
        self.pre_nodes[cur_edge.dir*self.N + cur_edge.nb] += 1;
    }

    fn push_post(&mut self, cur_edge:&StarEvent) {
        self.post_sum[map2D(incoming, cur_edge.dir)] += self.post_nodes[incoming*self.N + cur_edge.nb];
        self.post_sum[map2D(outgoing, cur_edge.dir)] += self.post_nodes[outgoing*self.N + cur_edge.nb];
        self.post_nodes[cur_edge.dir*self.N + cur_edge.nb] += 1;
    }

    fn pop_pre(&mut self, cur_edge:&StarEvent) {
        self.pre_sum[map2D(cur_edge.dir, incoming)] -= self.pre_nodes[incoming*self.N + cur_edge.nb];
        self.pre_sum[map2D(cur_edge.dir, outgoing)] -= self.pre_nodes[outgoing*self.N + cur_edge.nb];
    }

    fn pop_post(&mut self, cur_edge:&StarEvent) {
        self.post_sum[map2D(cur_edge.dir, incoming)] -= self.post_nodes[incoming*self.N + cur_edge.nb];
        self.post_sum[map2D(cur_edge.dir, outgoing)] -= self.post_nodes[outgoing*self.N + cur_edge.nb];
    }
    
    fn process_current(&mut self, cur_edge:&StarEvent) {
        self.mid_sum[map2D(incoming, cur_edge.dir)] -= self.pre_nodes[incoming*self.N + cur_edge.nb];
        self.mid_sum[map2D(outgoing, cur_edge.dir)] -= self.pre_nodes[outgoing*self.N + cur_edge.nb];

        for (d1,d2) in dirs2D {
            self.count_pre[map3D(d1, d2, cur_edge.dir)] += self.pre_sum[map2D(d1, d2)];
            self.count_post[map3D(cur_edge.dir, d1, d2)] += self.mid_sum[map2D(d1, d2)];
            self.count_mid[map3D(d1, cur_edge.dir, d2)] += self.mid_sum[map2D(d1, d2)];
        }

        self.mid_sum[map2D(cur_edge.dir, incoming)] += self.post_nodes[incoming*self.N + cur_edge.nb];
        self.mid_sum[map2D(cur_edge.dir, outgoing)] += self.post_nodes[outgoing*self.N + cur_edge.nb];
    }

    fn execute(&mut self, edges:&[StarEvent], delta:usize) {
        let L = edges.len();
        if L < 3 {
            return;
        }
        let mut start = 0;
        let mut end = 0;
        for j in 0..L {
            while start < L && edges[start].time + delta < edges[j].time {
                self.pop_pre(&edges[start]);
                start+=1;
            }
            while (end < L) && edges[end].time <= edges[j].time + delta {
                self.push_post(&edges[end]);
                end+=1;
            }
            self.pop_post(&edges[j]);
            self.process_current(&edges[j]);
            self.push_pre(&edges[j])
        }
    }

    fn concat_counts(&self) -> Vec<usize> {
        [self.count_pre,self.count_mid,self.count_post].concat()
    }
}
fn init_star_count(neighbours:Vec<usize>) -> StarCounter {
    let N = neighbours.len();
    StarCounter {N:N, pre_nodes: vec![0;2*N], post_nodes: vec![0;2*N], pre_sum: [0;8], mid_sum: [0;8], post_sum: [0;8],
         count_pre:[0;8], count_mid:[0;8], count_post:[0;8], final_counts: [0;8] }
}

// Triangle Motifs
struct TriangleEdge{
    uv_edge:bool,
    uorv:usize,
    nb:usize,
    dir:usize,
}
struct TriangleCounter {
    N:usize,
    pre_nodes:Vec<usize>,
    post_nodes:Vec<usize>,
    pre_sum:[usize;8],
    mid_sum:[usize;8],
    post_sum:[usize;8],
    final_counts:[usize;8],
}

impl TriangleCounter {
    fn push_pre(&mut self,cur_edge:&TriangleEdge) {
        let (isUorV, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.pre_sum[map3D(1-isUorV, incoming, dir)] += self.pre_nodes[self.N * map2D(incoming, 1 - isUorV) + nb];
            self.pre_sum[map3D(1-isUorV, outgoing, dir)] += self.pre_nodes[self.N * map2D(outgoing, 1 - isUorV) + nb];
            self.pre_nodes[self.N*map2D(dir, isUorV) + nb] +=1; 
        }
    }

    fn push_post(&mut self,cur_edge:&TriangleEdge) {
        let (isUorV, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.post_sum[map3D(1-isUorV, incoming, dir)] += self.post_nodes[self.N * map2D(incoming, 1 - isUorV) + nb];
            self.post_sum[map3D(1-isUorV, outgoing, dir)] += self.post_nodes[self.N * map2D(outgoing, 1 - isUorV) + nb];
            self.post_nodes[self.N*map2D(dir, isUorV) + nb] +=1; 
        }
    }

    fn pop_pre(&mut self, cur_edge:&TriangleEdge) {
        let (isUorV, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.pre_nodes[self.N * map2D(dir, isUorV) + nb] -= 1;
            self.pre_sum[map3D(isUorV, dir, incoming)] -= self.pre_nodes[self.N * map2D(incoming, 1-isUorV)];
            self.pre_sum[map3D(isUorV, dir, outgoing)] -= self.pre_nodes[self.N * map2D(outgoing, 1-isUorV)];       
        }
    }

    fn pop_post(&mut self, cur_edge:&TriangleEdge) {
        let (isUorV, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.post_nodes[self.N * map2D(dir, isUorV) + nb] -= 1;
            self.post_sum[map3D(isUorV, dir, incoming)] -= self.post_nodes[self.N * map2D(incoming, 1-isUorV)];
            self.post_sum[map3D(isUorV, dir, outgoing)] -= self.post_nodes[self.N * map2D(outgoing, 1-isUorV)];       
        }
    }

    fn process_current(&mut self, cur_edge:&TriangleEdge) {
        let (isUorV, nb, dir) = (cur_edge.uorv, cur_edge.nb, cur_edge.dir);
        if !cur_edge.uv_edge {
            self.mid_sum[map3D(1-isUorV, incoming, dir)] -= self.pre_nodes[self.N * map2D(incoming, 1-isUorV) + nb];
            self.mid_sum[map3D(1-isUorV, outgoing, dir)] -= self.pre_nodes[self.N * map2D(outgoing, 1-isUorV) + nb];
            self.mid_sum[map3D(isUorV, dir, incoming)] += self.post_nodes[self.N * map2D(incoming, 1-isUorV) + nb];
            self.mid_sum[map3D(isUorV, dir, outgoing)] += self.post_nodes[self.N * map2D(outgoing, 1-isUorV) + nb];
        }
        else {
            self.final_counts[0] += self.mid_sum[map3D(dir,0,0)] + self.post_sum[map3D(dir, 0, 1)] + self.pre_sum[map3D(1-dir,1,1)];
            self.final_counts[4] += self.mid_sum[map3D(dir,1,0)] + self.post_sum[map3D(1-dir, 0, 1)] + self.pre_sum[map3D(1-dir,0,1)];
            self.final_counts[2] += self.mid_sum[map3D(1-dir,0,0)] + self.post_sum[map3D(dir, 1, 1)] + self.pre_sum[map3D(1-dir,1,0)];
            self.final_counts[6] += self.mid_sum[map3D(1-dir,1,0)] + self.post_sum[map3D(1-dir, 1, 1)] + self.pre_sum[map3D(1-dir,0,0)];
            self.final_counts[1] += self.mid_sum[map3D(dir,0,1)] + self.post_sum[map3D(dir, 0, 0)] + self.pre_sum[map3D(dir,1,1)];
            self.final_counts[5] += self.mid_sum[map3D(dir,1,1)] + self.post_sum[map3D(1-dir, 0, 0)] + self.pre_sum[map3D(dir,0,1)];
            self.final_counts[3] += self.mid_sum[map3D(1-dir,0,1)] + self.post_sum[map3D(dir, 1, 0)] + self.pre_sum[map3D(dir,1,0)];
            self.final_counts[7] += self.mid_sum[map3D(1-dir,1,1)] + self.post_sum[map3D(1-dir, 1, 0)] + self.pre_sum[map3D(dir,0,0)];
        }
    }
}

#[cfg(test)]
mod three_node_motifs_test {
use super::{map2D, TwoNodeEvent,incoming,outgoing, TwoNodeCounter};

    #[test]
    fn map_test() {
    assert_eq!(map2D(1,1),3);
    }

    #[test]
    fn two_node_test() {
        let events = [TwoNodeEvent{dir:outgoing,time:1}, TwoNodeEvent{dir:outgoing,time:2}, TwoNodeEvent{dir:outgoing,time:3}];
        let mut twonc = TwoNodeCounter{count1d:[0;2],count2d:[0;4],count3d:[0;8]};
        twonc.execute(&events, 5);
        println!("motifs are {:?}",twonc.count3d);
    }


}