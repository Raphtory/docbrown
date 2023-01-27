use std::collections::HashSet;

use crate::graph::Triplet;
use crate::pages::vec;
use crate::pages::Page;
use crate::PAGE_SIZE;

type PageId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Location {
    page_id: PageId,
    offset: usize, // FIXME: this could probably be a lot smaller than 2^64
}

pub enum PageManagerError {
    NoFreePages,
    PageNotFound,
}

pub trait PageManager {
    type Page;

    fn new() -> Self;

    fn find_next_free_page(
        &self,
        initial_page: Option<&Location>,
    ) -> Result<Location, PageManagerError>;

    fn get_page_mut(&self, location: &Location) -> Option<&mut Self::Page>;
}

#[derive(Debug, Default)]
pub struct VecPageManager<T: Page> {
    pages: Vec<T>,
    free_pages: HashSet<usize>,
}

impl<T: Page> VecPageManager<T> {
}

impl<P: Page> PageManager for VecPageManager<P> {
    type Page = P;

    // when initial_page is None, find any free page
    // when initial_page is Some, return the page if it has free space,
    // otherwise follow the chain of overflow pages until we find a page that is not full
    fn find_next_free_page(
        &self,
        initial_page: Option<&Location>,
    ) -> Result<Location, PageManagerError> {
        match initial_page {
            Some(location) => {
                let page_id = location.page_id;
            }
            None => {

            },
        };
    }

    fn get_page_mut(&self, location: &Location) -> Option<&mut Self::Page> {
        self.pages.get_mut(location.page_id)
    }

    fn new() -> Self {
        Self { pages: Vec::new(), free_pages: HashSet::new() }
    }
}
