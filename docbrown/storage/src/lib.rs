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
        pages::{vec, CachedPage},
        Time, PAGE_SIZE,
    };

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Triplet<V, E>(V, Option<V>, Option<E>);

    impl<V, E> Triplet<V, E> {
        fn new(v: V, source: V, e: E) -> Self {
            Self(v, Some(source), Some(e))
        }
    }

    #[derive(Debug)]
    pub enum GraphError {
        PMError(PageManagerError),
    }

    type VecPage<V, E> = CachedPage<vec::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>;

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
        E: Ord,
        PM: PageManager<PageItem = VecPage<V, E>>,
    {
        pub fn neighbours_window(
            &self,
            w: Range<Time>,
            v: &V,
        ) -> Box<dyn Iterator<Item = (V, E)> + '_> {
            Box::new(std::iter::empty())
        }

        pub fn add_outbound_edge(
            &mut self,
            t: Time,
            src: V,
            dst: V,
            e: E,
        ) -> Result<(), GraphError> {
            if let Some(page_idx) = self.adj_list_locations.get(&src) {
                // the first page of the adjacency list for src exists, we need to call the page manager to get next free page
                // it could be the same or an overflow page

                let page_idx = self
                    .page_manager
                    .find_next_free_page(Some(page_idx))
                    .map_err(GraphError::PMError)?;

                if let Some(mut page) = self.page_manager.get_page_ref(&page_idx) {
                    page.data.append(Triplet::new(src, dst, e), t);
                    self.temporal_index.entry(t).or_default().insert(page_idx);
                    Ok(())
                } else {
                    Err(GraphError::PMError(PageManagerError::PageNotFound))
                }
            } else {
                let page_idx = self
                    .page_manager
                    .find_next_free_page(None)
                    .map_err(GraphError::PMError)?;

                if let Some(mut page) = self.page_manager.get_page_ref(&page_idx) {
                    page.data.append(Triplet::new(src.clone(), dst, e), t);
                    self.temporal_index.entry(t).or_default().insert(page_idx);
                    self.adj_list_locations.insert(src, page_idx);
                    Ok(())
                } else {
                    Err(GraphError::PMError(PageManagerError::PageNotFound))
                }
            }
        }
    }
}

#[cfg(test)]
mod paged_graph_tests {
    use super::*;

    #[test]
    fn test_insert_into_paged_graph() -> Result<(), graph::GraphError> {
        let mut graph = graph::PagedGraph::with_vec_page_manager();
        graph.add_outbound_edge(1, 1, 2, 1)?;
        graph.add_outbound_edge(2, 1, 3, 1)?;
        graph.add_outbound_edge(3, 1, 4, 2)?;
        graph.add_outbound_edge(4, 2, 5, 2)?;
        graph.add_outbound_edge(5, 3, 6, 2)?;

        let stats = graph.page_stats();

        assert_eq!(stats.num_pages, 2);
        assert_eq!(stats.num_free_pages, 1);

        println!("{:?}", graph);
        Ok(())
    }
}
