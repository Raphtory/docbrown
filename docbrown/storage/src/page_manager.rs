use std::collections::HashSet;
use std::marker::PhantomData;

use crate::pages::Page;
use crate::pages::PageRef;

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
    type PageItem: Page;

    fn new() -> Self;

    fn find_next_free_page(
        &self,
        initial_page: Option<&Location>,
    ) -> Result<Location, PageManagerError>;

    fn get_page_ref(&mut self, location: &Location) -> Option<PageRef<'_, Self::PageItem, Self>>
    where
        Self: Sized;

    fn get_page_mut(&mut self, location: PageId) -> Option<&mut Self::PageItem>;

    fn get_page(&self, page_id: PageId) -> Option<&Self::PageItem>;

    fn release_page(&mut self, location: &PageId) -> Result<(), PageManagerError>;
}

#[derive(Debug, Default)]
pub struct VecPageManager<T: Page> {
    pages: Vec<T>,
    free_pages: HashSet<usize>,
}

impl<T: Page> VecPageManager<T> {}

impl<P: Page> PageManager for VecPageManager<P> {
    type PageItem = P;

    // when initial_page is None, find any free page
    // when initial_page is Some, return the page if it has free space,
    // otherwise follow the chain of overflow pages until we find a page that is not full
    fn find_next_free_page(
        &self,
        initial_page: Option<&Location>,
    ) -> Result<Location, PageManagerError> {
        todo!()
    }

    // get the reference to the page at the given location
    // add it to the free pages if it is not full
    fn get_page_ref(&mut self, location: &Location) -> Option<PageRef<'_, P, Self>> {
        match self.pages.get_mut(location.page_id) {
            Some(_) => { 
                self.free_pages.insert(location.page_id);
                Some(PageRef::new(location.page_id, self))
            },
            None => None,
        }
    }

    fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Self::PageItem> {
        self.pages.get_mut(page_id)
    }

    fn new() -> Self {
        Self {
            pages: Vec::new(),
            free_pages: HashSet::new(),
        }
    }

    fn release_page(&mut self, page_id: &PageId) -> Result<(), PageManagerError> {
        if let Some(page) = self.get_page(*page_id) {
            if !page.is_full()  {
                self.free_pages.remove(page_id);
            }
            Ok(())
        } else {
            Err(PageManagerError::PageNotFound)
        }
    }

    fn get_page(&self, page_id: PageId) -> Option<&Self::PageItem> {
        self.pages.get(page_id)
    }
}
