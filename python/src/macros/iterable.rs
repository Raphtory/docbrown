macro_rules! py_numeric_iterable {
    ($name:ident, $item:ty, $iter:ty) => {
        py_iterable_base!($name, $item);
        py_iterable_methods!($name, $iter, $item);
        py_numeric_methods!($name, $item);
        py_ord_max_min_methods!($name, $item);
    };
}

macro_rules! py_float_iterable {
    ($name:ident, $item:ty, $iter:ty) => {
        py_iterable_base!($name, $item);

        py_iterable_methods!($name, $iter, $item);
        py_numeric_methods!($name, $item);
        py_float_max_min_methods!($name, $item);
    };
}

macro_rules! py_iterable {
    ($name:ident, $item:ty, $iter:ty) => {
        py_iterable_base!($name, $item);
        py_iterable_methods!($name, $iter, $item);
    };
}

macro_rules! py_iter_method {
    ($name:ident, $iter:ty) => {
        #[pymethods]
        impl $name {
            pub fn __iter__(&self) -> $iter {
                self.iter().into()
            }

            pub fn __len__(&self) -> usize {
                self.iter().count()
            }

            pub fn __repr__(&self) -> String {
                repr!(self.0)
            }
        }
    };
}
macro_rules! py_iterable_methods {
    ($name:ident, $iter:ty, $item:ty) => {
        py_iter_method!($name, $iter);

        #[pymethods]
        impl $name {
            pub fn collect(&self) -> Vec<$item> {
                self.iter().collect()
            }
        }
    };
}

macro_rules! py_numeric_methods {
    ($name:ident, $item:ty) => {
        #[pymethods]
        impl $name {
            pub fn sum(&self) -> $item {
                self.iter().sum()
            }

            pub fn mean(&self) -> f64 {
                use $crate::wrappers::iterators::MeanExt;
                self.iter().mean()
            }
        }
    };
}

macro_rules! py_ord_max_min_methods {
    ($name:ident, $item:ty) => {
        #[pymethods]
        impl $name {
            pub fn max(&self) -> Option<$item> {
                self.iter().max()
            }

            pub fn min(&self) -> Option<$item> {
                self.iter().min()
            }
        }
    };
}

macro_rules! py_float_max_min_methods {
    ($name:ident, $item:ty) => {
        #[pymethods]
        impl $name {
            pub fn max(&self) -> Option<$item> {
                self.iter().max_by(|a, b| a.total_cmp(b))
            }
            pub fn min(&self) -> Option<$item> {
                self.iter().min_by(|a, b| a.total_cmp(b))
            }
        }
    };
}

macro_rules! py_iterable_base {
    ($name:ident,$item:ty) => {
        #[pyclass]
        pub struct $name($crate::types::iterable::Iterable<$item>);

        impl Deref for $name {
            type Target = $crate::types::iterable::Iterable<$item>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<F: Fn() -> BoxedIter<$item> + Send + Sync + 'static> From<F> for $name {
            fn from(value: F) -> Self {
                Self($crate::types::iterable::Iterable::new(
                    stringify!($name).to_string(),
                    value,
                ))
            }
        }
    };
}
