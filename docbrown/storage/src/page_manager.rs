use std::collections::HashSet;

use crate::pages::Page;
use crate::pages::PageRef;

type PageId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Location {
    pub(crate) page_id: PageId,
    offset: usize, // FIXME: this could probably be a lot smaller than 2^64
}

impl Location {
    pub fn new(page_id: PageId, offset: usize) -> Self {
        Self { page_id, offset }
    }
}

#[derive(Debug)]
pub enum PageManagerError {
    NoFreePages,
    PageNotFound,
}

pub struct PageManagerStats {
    pub num_pages: usize,
    pub num_free_pages: usize,
}

pub trait PageManager {
    type PageItem: Page;

    fn new() -> Self;

    fn find_next_free_page(
        &mut self,
        initial_page: Option<&Location>,
    ) -> Result<Location, PageManagerError>;

    fn get_page_ref(&mut self, location: &Location) -> Option<PageRef<'_, Self::PageItem, Self>>
    where
        Self: Sized;


    fn get_pages_ref(&mut self, src_location: &Location, dst_location: &Location) -> (Option<PageRef<'_, Self::PageItem, Self>>, Option<PageRef<'_, Self::PageItem, Self>>)
    where
        Self: Sized;

    fn get_page_mut(&mut self, location: PageId) -> Option<&mut Self::PageItem>;

    fn get_page(&self, page_id: PageId) -> Option<&Self::PageItem>;

    fn release_page(&mut self, location: &PageId) -> Result<(), PageManagerError>;

    fn stats(&self) -> PageManagerStats;

    fn page_iter(
        &self,
        page_idx: &Location,
    ) -> Option<PageIter<'_, Self::PageItem, VecPageManager<Self::PageItem>>>;
}

#[derive(Debug, Default)]
pub struct VecPageManager<T: Page> {
    pages: Vec<T>,
    free_pages: HashSet<usize>,
}

impl<T: Page> VecPageManager<T> {
    fn get_location_or_new_page(
        &mut self,
        initial_page: &Location,
    ) -> Result<Result<Location, T>, PageManagerError> {
        let mut page_id = initial_page.page_id;

        let new_page_id = self.pages.len(); // because borrow checker

        let mut page = self
            .get_page_mut(page_id)
            .ok_or(PageManagerError::PageNotFound)?;

        while page.is_full() {
            if let Some(next_page_id) = page.overflow_page_id() {
                page_id = next_page_id;
                page = self
                    .get_page_mut(page_id)
                    .ok_or(PageManagerError::PageNotFound)?;
            } else {
                // create a new page and attach it to the last page as overflow
                let new_page = T::new(new_page_id);
                // self.pages.push(new_page);
                page.set_overflow_page_id(new_page_id);
                return Ok(Err(new_page));
            }
        }
        Ok(Ok(Location::new(page_id, page.next_free_offset())))
    }
}

impl<P: Page> PageManager for VecPageManager<P> {
    type PageItem = P;

    // when initial_page is None, find any free page and return the location
    // when initial_page is Some, return the page location if it has free space,
    // otherwise follow the chain of overflow pages until we find a page that is not full
    // if we can't find a page that is not full, create a new page attach it to the last page as overflow
    fn find_next_free_page(
        &mut self,
        initial_page: Option<&Location>,
    ) -> Result<Location, PageManagerError> {
        if let Some(initial_page) = initial_page {
            let result = self.get_location_or_new_page(initial_page)?;
            match result {
                Ok(location) => Ok(location),
                Err(new_page) => {
                    let loc = Location::new(new_page.page_id(), 0);
                    self.pages.push(new_page);
                    Ok(loc)
                }
            }
        } else {
            // when initial_page is None first search free_pages for a page that is not full
            for page_id in &self.free_pages {
                if let Some(page) = self.get_page(*page_id) {
                    if !page.is_full() {
                        return Ok(Location::new(*page_id, page.next_free_offset()));
                    }
                }
            }
            // if we can't find a page that is not full, create a new page
            let new_page_id = self.pages.len();
            let new_page = P::new(new_page_id);
            self.pages.push(new_page);
            self.free_pages.insert(new_page_id);
            Ok(Location::new(new_page_id, 0))
        }
    }

    // get the reference to the page at the given location
    // add it to the free pages if it is not full
    fn get_page_ref(&mut self, location: &Location) -> Option<PageRef<'_, P, Self>> {
        match self.pages.get_mut(location.page_id) {
            Some(_) => {
                self.free_pages.insert(location.page_id);
                Some(PageRef::new(location.page_id, self))
            }
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
            if page.is_full() {
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

    fn stats(&self) -> PageManagerStats {
        PageManagerStats {
            num_pages: self.pages.len(),
            num_free_pages: self.free_pages.len(),
        }
    }

    fn page_iter(
        &self,
        page_idx: &Location,
    ) -> Option<PageIter<'_, Self::PageItem, VecPageManager<Self::PageItem>>> {
        let _ = self.get_page(page_idx.page_id)?;
        Some(PageIter::new(self, page_idx.page_id))
    }

    fn get_pages_ref(&mut self, src_location: &Location, dst_location: &Location) -> (Option<PageRef<'_, Self::PageItem, Self>>, Option<PageRef<'_, Self::PageItem, Self>>)
    where
        Self: Sized {
        todo!()
    }
}

pub struct PageIter<'a, P: Page, PM: PageManager<PageItem = P>> {
    page_manager: &'a PM,
    page_id: Option<PageId>,
}

impl<'a, P, PM> PageIter<'a, P, PM>
where
    P: Page,
    PM: PageManager<PageItem = P>,
{
    fn new(page_manager: &'a PM, page_id: PageId) -> Self {
        Self {
            page_manager,
            page_id: Some(page_id),
        }
    }
}

impl<'a, P, PM> Iterator for PageIter<'a, P, PM>
where
    P: Page + 'a,
    PM: PageManager<PageItem = P>,
{
    type Item = &'a P;

    // given a starting page location follow the overflow chain and return the pages
    // if the page location doesn't exist return None
    fn next(&mut self) -> Option<Self::Item> {
        let page_id = self.page_id?;
        let page = self.page_manager.get_page(page_id)?;
        self.page_id = page.overflow_page_id();
        Some(page)
    }
}
