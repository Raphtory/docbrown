macro_rules! py_nested_iterable_base {
    ($name:ident, $item:ty) => {
        #[pyclass]
        pub struct $name {
            builder:
                std::sync::Arc<dyn Fn() -> BoxedIter<BoxedIter<$item>> + Send + Sync + 'static>,
        }

        impl $name {
            fn iter(&self) -> BoxedIter<BoxedIter<$item>> {
                (self.builder)()
            }
        }

        impl<F: Fn() -> BoxedIter<BoxedIter<$item>> + Sync + Send + 'static> From<F> for $name {
            fn from(value: F) -> Self {
                Self {
                    builder: std::sync::Arc::new(value),
                }
            }
        }
    };
}

macro_rules! py_nested_iterable_methods {
    ($name:ident, $item:ty, $iter:ty) => {
        py_iter_method!($name, $iter);

        #[pymethods]
        impl $name {
            pub fn collect(&self) -> Vec<Vec<$item>> {
                self.iter().map(|it| it.collect()).collect()
            }
        }
    };
}

macro_rules! py_nested_numeric_methods {
    ($name:ident, $item:ty, $value_iterable:ty) => {
        #[pymethods]
        impl $name {
            pub fn sum(&self) -> $value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<$item> = Box::new(builder().map(|it| it.sum()));
                    iter
                })
                .into()
            }

            pub fn mean(&self) -> Float64Iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<f64> = Box::new(builder().map(|it| it.mean()));
                    iter
                })
                .into()
            }
        }
    };
}

macro_rules! py_nested_iterable {
    ($name:ident, $item:ty, $iter:ty, $value_iterable:ty) => {
        py_nested_iterable_base!($name, $item);
        py_nested_iterable_methods!($name, $item, $iter);
    };
}

macro_rules! py_nested_numeric_iterable {
    ($name:ident, $item:ty, $iter:ty, $value_iterable:ty, $option_value_iterable:ty) => {
        py_nested_iterable_base!($name, $item);

        py_nested_iterable_methods!($name, $item, $iter);
        py_nested_numeric_methods!($name, $item, $value_iterable);

        #[pymethods]
        impl $name {
            fn max(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> = Box::new(builder().map(|it| it.max()));
                    iter
                })
                .into()
            }

            fn min(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> = Box::new(builder().map(|it| it.min()));
                    iter
                })
                .into()
            }
        }
    };
}
