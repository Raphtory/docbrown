use std::ptr::addr_of;

pub struct ReprWrapper<'a, T> {
    value: &'a T,
}

impl<'a, T> ReprWrapper<'a, T> {
    pub fn new(value: &'a T) -> Self {
        Self { value }
    }

    pub fn get(&self) -> &T {
        self.value
    }
}

impl<T> Repr for ReprWrapper<'_, T> {
    fn repr(&self) -> String {
        let val = self.get();
        format!("Unknown object {:?}", addr_of!(val))
    }
}

impl<T: ToString> Repr for &ReprWrapper<'_, T> {
    fn repr(&self) -> String {
        self.get().to_string()
    }
}

pub trait Repr {
    fn repr(&self) -> String;
}

impl<T> Repr for &&ReprWrapper<'_, Option<T>> {
    fn repr(&self) -> String {
        match self.get() {
            Some(v) => repr!(v),
            None => "None".to_string(),
        }
    }
}
