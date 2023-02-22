fn main() {}

#[cfg(test)]
mod test {
    use std::{
        ops::Deref,
        path::{Path, PathBuf},
    };

    use docbrown_core::Direction;
    use docbrown_db::{
        csv_loader::csv::CsvLoader,
        graph::Graph,
        view_api::{internal::GraphViewInternalOps, GraphViewOps, VertexViewOps},
    };
    use rand::Rng;
    use serde::de::DeserializeOwned;

    fn load<REC: DeserializeOwned>(g1: &Graph, gn: &Graph, p: PathBuf) {
        CsvLoader::new(p)
            .set_delimiter(" ")
            .load_into_graph(&(g1, gn), |pair: Pair, (g1, gn)| {
                // let mut rng = rand::thread_rng();
                // let t = rng.gen_range(-100..100);
                // println!("{} {} {}", pair.src, pair.dst, t);
                g1.add_edge(pair.t, pair.src, pair.dst, &vec![]);
                gn.add_edge(pair.t, pair.src, pair.dst, &vec![]);
            })
            .expect("Failed to load graph from CSV files");
    }

    fn test_graph_sanity<P, REC: DeserializeOwned>(p: P, n_parts: usize)
    where
        P: Into<PathBuf>,
    {
        let path: PathBuf = p.into();
        let g1 = Graph::new(1);
        let gn = Graph::new(n_parts);

        load::<REC>(&g1, &gn, path.clone());

        assert_eq!(g1.vertices_len(), gn.vertices_len());
        // NON-TEMPORAL TESTS HERE!

        let mut expect_1 = g1.vertex_refs().map(|v| v.g_id).collect::<Vec<_>>();
        let mut expect_n = gn.vertex_refs().map(|v| v.g_id).collect::<Vec<_>>();

        expect_1.sort();
        expect_n.sort();

        assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

        for v_ref in g1.vertex_refs() {
            let v1 = g1.vertex(v_ref.g_id).unwrap().id();
            let vn = gn.vertex(v_ref.g_id).unwrap().id();

            assert_eq!(v1, vn, "Graphs are not equal {n_parts}");
            let v_id = v1;
            for d in vec![Direction::OUT, Direction::IN, Direction::BOTH] {
                let mut expect_1 = g1
                    .neighbours(v_id.into(), d)
                    .map(|id| id.g_id)
                    .collect::<Vec<_>>();

                let mut expect_n = gn
                    .neighbours(v_id.into(), d)
                    .map(|id| id.g_id)
                    .collect::<Vec<_>>();

                expect_1.sort();
                expect_n.sort();

                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

                // now we test degrees
                let expect_1 = g1.degree(v_id.into(), d);
                let expect_n = gn.degree(v_id.into(), d);

                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts} {d:?}");
            }
        }

        // TEMPORAL TESTS HERE!
        let t_start = 0;
        let t_end = 100;

        let mut expected_1 = g1
            .vertex_refs_window(t_start, t_end)
            .map(|v| v.g_id)
            .collect::<Vec<_>>();
        let mut expected_n = gn
            .vertex_refs_window(t_start, t_end)
            .map(|v| v.g_id)
            .collect::<Vec<_>>();

        expected_1.sort();
        expected_n.sort();

        assert_eq!(expected_1, expected_n, "Graphs are not equal {n_parts}");

        for v_ref in g1.vertex_refs_window(t_start, t_end) {
            let v1 = g1.vertex(v_ref.g_id).unwrap().id();
            let vn = gn.vertex(v_ref.g_id).unwrap().id();

            assert_eq!(v1, vn, "Graphs are not equal {n_parts}");
            let v_id = v1;
            for d in vec![Direction::OUT, Direction::IN, Direction::BOTH] {
                let mut expected_1 = g1
                    .neighbours_window(v_id.into(), t_start, t_end, d)
                    .map(|id| id.g_id)
                    .collect::<Vec<_>>();

                let mut expected_n = gn
                    .neighbours_window(v_id.into(), t_start, t_end, d)
                    .map(|id| id.g_id)
                    .collect::<Vec<_>>();

                expected_1.sort();
                expected_n.sort();

                assert_eq!(expected_1, expected_n, "Graphs are not equal {n_parts}");

                // now we test degrees
                let expected_1 = g1.degree_window(v_id.into(), t_start, t_end, d);
                let expected_n = gn.degree_window(v_id.into(), t_start, t_end, d);

                assert_eq!(
                    expected_1, expected_n,
                    "Graphs are not equal {n_parts} {d:?}"
                );
            }
        }

        let mut expected_1 = g1
            .vertex_refs_window(t_start, t_end)
            .map(|id| {
                let deg = g1.degree_window(id, t_start, t_end, docbrown_core::Direction::BOTH);
                let out_deg = g1.degree_window(id, t_start, t_end, docbrown_core::Direction::OUT);
                let in_deg = g1.degree_window(id, t_start, t_end, docbrown_core::Direction::IN);
                (id.g_id, deg, out_deg, in_deg)
            })
            .collect::<Vec<_>>();
        expected_1.sort_by(|v1, v2| v1.0.cmp(&v2.0));

        let mut expected_n = gn
            .vertex_refs_window(t_start, t_end)
            .map(|id| {
                let deg = gn.degree_window(id, t_start, t_end, docbrown_core::Direction::BOTH);
                let out_deg = gn.degree_window(id, t_start, t_end, docbrown_core::Direction::OUT);
                let in_deg = gn.degree_window(id, t_start, t_end, docbrown_core::Direction::IN);
                (id.g_id, deg, out_deg, in_deg)
            })
            .collect::<Vec<_>>();
        expected_n.sort_by(|v1, v2| v1.0.cmp(&v2.0));

        assert!(expected_1.len() > 0, "Graph is empty {n_parts}");
        assert!(expected_n.len() > 0, "Graph is empty {n_parts}");
        assert_eq!(expected_1, expected_n, "Graphs are not equal {n_parts}");

        let wg1 = g1.window(t_start, t_end);
        let wgn = gn.window(t_start, t_end);

        assert_eq!(wg1.vertices_len(), wgn.vertices_len());

        let mut expected_1 = wg1
            .vertices()
            .map(|vs| (vs.id(), vs.degree(), vs.out_degree(), vs.in_degree()))
            .collect::<Vec<_>>();
        expected_1.sort_by(|v1, v2| v1.0.cmp(&v2.0));

        let mut expected_n = wgn
            .vertices()
            .map(|vs| (vs.id(), vs.degree(), vs.out_degree(), vs.in_degree()))
            .collect::<Vec<_>>();

        expected_n.sort_by(|v1, v2| v1.0.cmp(&v2.0));

        assert_eq!(expected_1, expected_n, "Graphs are not equal {n_parts}");
    }

    #[derive(serde::Deserialize, std::fmt::Debug)]
    struct Pair {
        src: u64,
        dst: u64,
        t: i64,
    }

    #[test]
    fn load_graph_from_cargo_path() {
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/", "test2.csv"]
            .iter()
            .collect();

        let p = Path::new(&csv_path);
        println!("Path: {}", p.display());
        assert!(p.exists());

        for n_parts in 1..33 {
            test_graph_sanity::<&Path, Pair>(p, n_parts);
        }
    }
}
