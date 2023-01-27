mod page_manager;
mod pages;

pub const PAGE_SIZE: usize = 4;

pub mod graph {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use crate::{
        page_manager::{Location, PageManager, PageManagerError, VecPageManager},
        pages::vec,
        PAGE_SIZE,
    };

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Triplet<V, E>(V, Option<V>, Option<E>);

    impl<V, E> Triplet<V, E> {
        fn new(v: V, source: V, e: E) -> Self {
            Self(v, Some(source), Some(e))
        }
    }

    pub enum GraphError {
        PMError(PageManagerError),
    }

    #[derive(Debug, Default)]
    pub struct PagedGraph<
        V,
        E,
        PM: PageManager<Page = vec::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>,
    > {
        // pages: Vec<vec::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>,
        page_manager: PM,
        // this holds the mapping from the timestamp to the page location
        temporal_index: BTreeMap<i64, BTreeSet<Location>>,

        // this holds the mapping from the external vertex id to the page location
        global_to_logical_map: HashMap<V, Location>,
    }

    impl<V, E>
        PagedGraph<V, E, VecPageManager<vec::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>>
    {
        pub fn with_vec_page_manager() -> Self {
            Self {
                page_manager: VecPageManager::new(),
                temporal_index: BTreeMap::new(),
                global_to_logical_map: HashMap::new(),
            }
        }
    }

    impl<V, E, PM> PagedGraph<V, E, PM>
    where
        V: Ord + std::hash::Hash + Clone,
        E: Ord,
        PM: PageManager<Page = vec::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>,
    {
        fn new() -> Self {
            Self {
                page_manager: PM::new(),
                temporal_index: BTreeMap::new(),
                global_to_logical_map: HashMap::new(),
            }
        }
        // we follow the chain of overflow pages until we find a page that is not full
        // if we reach the end of the chain, and the last page is perfectly full
        // we return Err(last_page_id) so we can create a new page and add it to the chain
        fn find_free_page(&self, page_id: usize) -> Result<usize, usize> {
            // // FIXME: this is problematic as we'll be forced to lift every page in the chain, just to check if it's full
            // let page = &self.pages[page_id];
            // if !page.is_full() {
            //     return Ok(page_id);
            // } else if let Some(overflow_page_id) = page.overflow_page_id() {
            //     return self.find_free_page(overflow_page_id);
            // } else {
            //     return Err(page_id);
            // }
            Err(0)
        }

        pub fn add_outbound_edge(
            &mut self,
            t: i64,
            src: V,
            dst: V,
            e: E,
        ) -> Result<(), GraphError> {
            if let Some(page_idx) = self.global_to_logical_map.get(&src) {
                // the first page of the adjacency list for src exists, we need to call the page manager to get next free page
                // it could be the same or an overflow page

                let page_idx = self
                    .page_manager
                    .find_next_free_page(Some(page_idx))
                    .map_err(GraphError::PMError)?;

                if let Some(page) = self.page_manager.get_page_mut(&page_idx) {
                    page.append(Triplet::new(src, dst, e), t);
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

                if let Some(page) = self.page_manager.get_page_mut(&page_idx) {
                    page.append(Triplet::new(src.clone(), dst, e), t);
                    self.temporal_index.entry(t).or_default().insert(page_idx);
                    self.global_to_logical_map.insert(src, page_idx);
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
    fn test_paged_graph() {
        let mut graph = graph::PagedGraph::with_vec_page_manager();
        graph.add_outbound_edge(1, 1, 2, 1);
        graph.add_outbound_edge(2, 1, 3, 1);
        graph.add_outbound_edge(3, 1, 4, 2);
        graph.add_outbound_edge(4, 2, 5, 2);
        graph.add_outbound_edge(5, 3, 6, 2);

        println!("{:?}", graph);
    }
}
