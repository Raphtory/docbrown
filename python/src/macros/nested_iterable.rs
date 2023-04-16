macro_rules! py_nested_iterable_base {
    ($name:ident, $item:ty) => {
        #[pyclass]
        pub struct $name($crate::types::iterable::NestedIterable<$item>);

        impl Deref for $name {
            type Target = $crate::types::iterable::NestedIterable<$item>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<F: Fn() -> BoxedIter<BoxedIter<$item>> + Sync + Send + 'static> From<F> for $name {
            fn from(value: F) -> Self {
                Self($crate::types::iterable::NestedIterable::new(
                    stringify!($name).to_string(),
                    value,
                ))
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

macro_rules! py_nested_ord_max_min_methods {
    ($name:ident, $item:ty, $option_value_iterable:ty) => {
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

macro_rules! py_nested_float_max_min_methods {
    ($name:ident, $item:ty, $option_value_iterable:ty) => {
        #[pymethods]
        impl $name {
            fn max(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> =
                        Box::new(builder().map(|it| it.max_by(|a, b| a.total_cmp(b))));
                    iter
                })
                .into()
            }

            fn min(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> =
                        Box::new(builder().map(|it| it.min_by(|a, b| a.total_cmp(b))));
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
        py_nested_ord_max_min_methods!($name, $item, $option_value_iterable);
    };
}

macro_rules! py_nested_float_iterable {
    ($name:ident, $item:ty, $iter:ty, $value_iterable:ty, $option_value_iterable:ty) => {
        py_nested_iterable_base!($name, $item);

        py_nested_iterable_methods!($name, $item, $iter);
        py_nested_numeric_methods!($name, $item, $value_iterable);
        py_nested_float_max_min_methods!($name, $item, $option_value_iterable);
    };
}
