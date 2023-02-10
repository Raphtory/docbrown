mod arrow_storage;
mod page_manager;
mod pages;

pub const PAGE_SIZE: usize = 4;

type Time = i64;

pub mod graph {
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        ops::Range,
    };

    use crate::{
        page_manager::{Location, PageManager, PageManagerError, PageManagerStats, VecPageManager},
        pages::{self, CachedPage, Page, PageError},
        Time, PAGE_SIZE,
    };

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Triplet<V, E> {
        pub source: V,
        pub vertex: Option<V>, // this is the vertex that is being pointed to
        pub edge: Option<E>,
    }

    #[derive(Clone, Copy)]
    pub enum Direction {
        Inbound,
        Outbound,
        Both,
    }

    impl<V, E> Triplet<V, E> {
        pub fn new(source: V, vertex: V, e: E) -> Self {
            Self {
                source,
                vertex: Some(vertex),
                edge: Some(e),
            }
        }
    }

    impl<V: Clone, E> Triplet<V, E> {
        pub fn prefix(v: &V) -> Self {
            Self {
                source: v.clone(),
                vertex: None,
                edge: None,
            }
        }
    }

    #[derive(Debug)]
    pub enum GraphError {
        PageError(PageError),
        PMError(PageManagerError),
    }

    type VecPage<V, E> =
        CachedPage<pages::vec_page::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>;

    #[derive(Debug, Default)]
    pub struct PagedGraph<V, E, PM: PageManager<PageItem = VecPage<V, E>>> {
        // manages the pages like a true manager that is all.. managing you know .. pages
        page_manager: PM,
        // this holds the mapping from the timestamp to the page location
        temporal_index: BTreeMap<Time, BTreeSet<Location>>,
        // this holds the mapping from the external vertex id to the page holding the adjacency list
        // if a vertex doesn't have an adjacency list, it will be None
        adj_list_locations: HashMap<V, Location>,
    }

    impl<V: Ord, E: Ord> PagedGraph<V, E, VecPageManager<VecPage<V, E>>> {
        pub fn with_vec_page_manager() -> Self {
            Self {
                page_manager: VecPageManager::new(),
                temporal_index: BTreeMap::new(),
                adj_list_locations: HashMap::new(),
            }
        }

        pub(crate) fn page_stats(&self) -> PageManagerStats {
            self.page_manager.stats()
        }
    }

    impl<V, E, PM> PagedGraph<V, E, PM>
    where
        V: Ord + std::hash::Hash + Clone,
        E: Ord + Clone,
        PM: PageManager<PageItem = VecPage<V, E>>,
    {
        pub fn neighbours_window(
            &self,
            w: Range<Time>,
            v: &V,
            d: Direction,
        ) -> Option<Box<dyn Iterator<Item = (&V, &E)> + '_>> {
            // this API is kinda awkward, but it's the best I could come up with for the POC
            // TODO: we could filter any page that doesn't find v
            let local_v = v.clone();
            let page_idx = self.adj_list_locations.get(v)?;
            let pages = self.page_manager.page_iter(page_idx)?;
            let iter = pages.flat_map(move |page| {
                let prefix = Triplet::prefix(&local_v);
                let v2 = local_v.clone();
                page.data
                    .tuples_window_for_source(w.clone(), d.clone(), &prefix, move |t| t.source == v2)
                    .map(|(_, t)| (t.vertex.as_ref().unwrap(), t.edge.as_ref().unwrap()))
            });
            Some(Box::new(iter))
        }

        pub fn add_edge(&mut self, t: Time, src: V, dst: V, e: E) -> Result<(), GraphError> {
            let src_location = self.adj_list_locations.get(&src);
            let dst_location = self.adj_list_locations.get(&dst);

            let src_page_idx = self
                .page_manager
                .find_next_free_page(src_location)
                .map_err(GraphError::PMError)?;

            let dst_page_idx = self
                .page_manager
                .find_next_free_page(dst_location)
                .map_err(GraphError::PMError)?;

            if let Some(mut src_page) = self.page_manager.get_page_ref(&src_page_idx) {
                src_page
                    .data
                    .append_out(
                        Triplet::new(src.clone(), dst.clone(), e.clone()),
                        t,
                        dst_page_idx.page_id.try_into().unwrap(),
                    )
                    .map_err(GraphError::PageError)?;

                self.temporal_index
                    .entry(t)
                    .or_default()
                    .insert(src_page_idx);

                self.adj_list_locations
                    .entry(src.clone())
                    .or_insert(src_page_idx);
            } else {
                return Err(GraphError::PMError(PageManagerError::PageNotFound));
            }

            if let Some(mut dst_page) = self.page_manager.get_page_ref(&dst_page_idx) {
                dst_page
                    .data
                    .append_in(
                        Triplet::new(src.clone(), dst.clone(), e.clone()),
                        t,
                        src_page_idx.page_id.try_into().unwrap(),
                    )
                    .map_err(GraphError::PageError)?;

                self.temporal_index
                    .entry(t)
                    .or_default()
                    .insert(dst_page_idx);

                self.adj_list_locations
                    .entry(dst.clone())
                    .or_insert(dst_page_idx);
            } else {
                return Err(GraphError::PMError(PageManagerError::PageNotFound));
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod paged_graph_tests {
    use super::*;

    #[test]
    fn test_insert_into_paged_graph() -> Result<(), graph::GraphError> {
        let mut graph = graph::PagedGraph::with_vec_page_manager();
        graph.add_edge(1, 1, 2, 1)?;
        graph.add_edge(4, 2, 5, 2)?;
        graph.add_edge(5, 3, 6, 2)?;
        graph.add_edge(2, 1, 3, 1)?;
        graph.add_edge(3, 1, 3, 2)?;

        let stats = graph.page_stats();

        assert_eq!(stats.num_pages, 3);
        assert_eq!(stats.num_free_pages, 1);

        // test outbound for src = 1
        let actual = graph
            .neighbours_window(1..6, &1, graph::Direction::Outbound)
            .unwrap()
            .map(|(v, e)| (*v, *e))
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(2, 1), (3, 1), (3, 2)]);
        // test outbound for src = 2
        let actual = graph
            .neighbours_window(1..6, &2, graph::Direction::Outbound)
            .unwrap()
            .map(|(v, e)| (*v, *e))
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(5, 2)]);

        //test outbound for src = 3
        let actual = graph
            .neighbours_window(1..6, &3, graph::Direction::Outbound)
            .unwrap()
            .map(|(v, e)| (*v, *e))
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![(6, 2)]);

        //test outbound for src = 4
        let actual = graph.neighbours_window(1..6, &4, graph::Direction::Outbound);

        assert!(actual.is_none());

        Ok(())
    }

    #[test]
    fn test_insert_into_paged_graph_with_overflow() -> Result<(), graph::GraphError> {
        let mut graph = graph::PagedGraph::with_vec_page_manager();
        graph.add_edge(1, 1, 2, 1)?;
        graph.add_edge(2, 1, 3, 1)?;
        graph.add_edge(3, 1, 4, 2)?;
        graph.add_edge(4, 1, 5, 2)?;
        graph.add_edge(5, 1, 6, 2)?;

        let stats = graph.page_stats();

        assert_eq!(stats.num_pages, 3);
        assert_eq!(stats.num_free_pages, 1);

        println!("{:?}", graph);
        Ok(())
    }
}
