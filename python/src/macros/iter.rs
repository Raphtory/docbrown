macro_rules! py_iterator {
    ($name:ident, $item:ty) => {
        #[pyclass]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $item> + Send>,
        }

        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }
            fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<$item> {
                slf.iter.next()
            }
        }

        impl From<Box<dyn Iterator<Item = $item> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $item> + Send>) -> Self {
                Self { iter: value }
            }
        }

        impl IntoIterator for $name {
            type Item = $item;
            type IntoIter = Box<dyn Iterator<Item = $item> + Send>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter
            }
        }
    };

    ($name:ident, $item:ty, $pyitem:ty) => {
        #[pyclass]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $pyitem> + Send>,
        }

        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }
            fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<$pyitem> {
                slf.iter.next()
            }
        }

        impl From<Box<dyn Iterator<Item = $item> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $item> + Send>) -> Self {
                let iter = Box::new(value.map(|v| v.into()));
                Self { iter }
            }
        }

        impl From<Box<dyn Iterator<Item = $pyitem> + Send>> for $name {
            fn from(value: Box<dyn Iterator<Item = $pyitem> + Send>) -> Self {
                Self { iter: value }
            }
        }

        impl IntoIterator for $name {
            type Item = $pyitem;
            type IntoIter = Box<dyn Iterator<Item = $pyitem> + Send>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter
            }
        }
    };
}
