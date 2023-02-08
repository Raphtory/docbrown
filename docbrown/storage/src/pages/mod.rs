pub(crate) mod arrow_page;
pub(crate) mod vec_page;


use std::ops::{Deref, DerefMut};

use crate::page_manager::PageManager;

pub type PageId = u32;

pub trait Page {
    fn page_id(&self) -> usize;
    fn is_full(&self) -> bool;
    fn overflow_page_id(&self) -> Option<usize>;
    fn set_overflow_page_id(&mut self, page_id: usize);
    fn next_free_offset(&self) -> usize;

    fn new(page_id: usize) -> Self;
}

trait PageData {
    fn new() -> Self;
    fn is_full(&self) -> bool;
    fn next_free_offset(&self) -> usize;
}

#[derive(Debug)]
pub struct CachedPage<T> {
    page_id: usize,
    overflow_page_id: Option<usize>,
    pub(crate) data: T,
}

impl<T: PageData> Page for CachedPage<T> {
    fn page_id(&self) -> usize {
        self.page_id
    }

    fn overflow_page_id(&self) -> Option<usize> {
        self.overflow_page_id
    }

    fn next_free_offset(&self) -> usize {
        self.data.next_free_offset()
    }

    fn is_full(&self) -> bool {
        self.data.is_full()
    }

    fn set_overflow_page_id(&mut self, page_id: usize) {
        self.overflow_page_id = Some(page_id);
    }

    fn new(page_id: usize) -> Self {
        Self {
            page_id,
            overflow_page_id: None,
            data: T::new(),
        }
    }
}

pub struct PageRef<'a, T: Page, PM>
where
    PM: PageManager<PageItem = T>,
{
    page_id: usize,
    pm: &'a mut PM,
    _a: std::marker::PhantomData<T>,
}

impl<'a, T, PM> PageRef<'a, T, PM>
where
    T: Page,
    PM: PageManager<PageItem = T>,
{
    pub fn new(page_id: usize, pm: &'a mut PM) -> Self {
        Self {
            page_id,
            pm,
            _a: std::marker::PhantomData,
        }
    }
}

impl<T, PM> Drop for PageRef<'_, T, PM>
where
    T: Page,
    PM: PageManager<PageItem = T>,
{
    fn drop(&mut self) {
        (&mut self.pm)
            .release_page(&self.page_id)
            .expect(format!("Page {} not found", self.page_id).as_str());
    }
}

impl<T, PM> Deref for PageRef<'_, T, PM>
where
    T: Page,
    PM: PageManager<PageItem = T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.pm.get_page(self.page_id).unwrap()
    }
}

impl<T, PM> DerefMut for PageRef<'_, T, PM>
where
    T: Page,
    PM: PageManager<PageItem = T>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.pm.get_page_mut(self.page_id).unwrap()
    }
}

#[derive(Debug, PartialEq)]
pub enum PageError {
    PageFull,
}
