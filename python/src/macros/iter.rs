macro_rules! py_iterator_struct {
    ($name:ident, $pyitem:ty) => {
        #[pyclass]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $pyitem> + Send>,
        }
    };
    ($name:ident, $pyname:literal, $pyitem:ty) => {
        #[pyclass(name=$pyname)]
        pub struct $name {
            iter: Box<dyn Iterator<Item = $pyitem> + Send>,
        }
    };
}

macro_rules! py_iterator_methods {
    ($name:ident, $item:ty, $pyitem:ty) => {
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

        // impl From<Box<dyn Iterator<Item = $pyitem> + Send>> for $name {
        //     fn from(value: Box<dyn Iterator<Item = $pyitem> + Send>) -> Self {
        //         Self { iter: value }
        //     }
        // }

        impl IntoIterator for $name {
            type Item = $pyitem;
            type IntoIter = Box<dyn Iterator<Item = $pyitem> + Send>;

            fn into_iter(self) -> Self::IntoIter {
                self.iter
            }
        }
    };
}

/// Construct a python Iterator struct
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`)
macro_rules! py_iterator {
    ($name:ident, $item:ty) => {
        py_iterator_struct!($name, $item);
        py_iterator_methods!($name, $item, $item);
    };
    ($name:ident, $item:ty, $pyitem:ty) => {
        py_iterator_struct!($name, $pyitem);
        py_iterator_methods!($name, $item, $pyitem);
    };
    ($name:ident, $item:ty, $pyitem:ty, $pyname:literal) => {
        py_iterator_struct!($name, $pyname, $pyitem);
        py_iterator_methods!($name, $item, $pyitem);
    };
}
