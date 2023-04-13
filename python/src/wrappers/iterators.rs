use crate::wrappers::prop::{PropHistories, PropHistory, PropValue, Props};
use docbrown::core as db_c;
use docbrown::db::view_api::BoxedIter;
use num::cast::AsPrimitive;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::i64;
use std::iter::Sum;

pub(crate) trait MeanExt<V>: Iterator<Item = V>
where
    V: AsPrimitive<f64> + Sum<V>,
{
    fn mean(self) -> f64
    where
        Self: Sized,
    {
        let mut count: usize = 0;
        let sum: V = self.inspect(|_| count += 1).sum();

        if count > 0 {
            sum.as_() / (count as f64)
        } else {
            0.0
        }
    }
}

impl<I: ?Sized + Iterator<Item = V>, V: AsPrimitive<f64> + Sum<V>> MeanExt<V> for I {}

py_iterator!(Float64Iter, f64);
py_float_iterable!(Float64Iterable, f64, Float64Iter);

py_iterator!(U64Iter, u64);
py_numeric_iterable!(U64Iterable, u64, U64Iter);
py_iterator!(NestedU64Iter, BoxedIter<u64>, U64Iter);
py_nested_numeric_iterable!(
    NestedU64Iterable,
    u64,
    NestedU64Iter,
    U64Iterable,
    OptionU64Iterable
);

py_iterator!(OptionU64Iter, Option<u64>);
py_iterable!(OptionU64Iterable, Option<u64>, OptionU64Iter);
py_ord_max_min_methods!(OptionU64Iterable, Option<u64>);

py_iterator!(I64Iter, i64);
py_numeric_iterable!(I64Iterable, i64, I64Iter);
py_iterator!(NestedI64Iter, BoxedIter<i64>, I64Iter);
py_nested_numeric_iterable!(
    NestedI64Iterable,
    i64,
    NestedI64Iter,
    I64Iterable,
    OptionI64Iterable
);

py_iterator!(OptionI64Iter, Option<i64>);
py_iterable!(OptionI64Iterable, Option<i64>, OptionI64Iter);
py_ord_max_min_methods!(OptionI64Iterable, Option<i64>);
py_iterator!(NestedOptionI64Iter, BoxedIter<Option<i64>>, OptionI64Iter);
py_nested_iterable!(
    NestedOptionI64Iterable,
    Option<i64>,
    NestedOptionI64Iter,
    OptionI64Iterable
);

py_iterator!(UsizeIter, usize);
py_iterator!(NestedUsizeIter, BoxedIter<usize>, UsizeIter);

py_iterator!(BoolIter, bool);
py_iterator!(NestedBoolIter, BoxedIter<bool>, BoolIter);

py_iterator!(StringIter, String);
py_iterator!(NestedStringIter, BoxedIter<String>, StringIter);

py_iterator!(StringVecIter, Vec<String>);
py_iterator!(NestedStringVecIter, BoxedIter<Vec<String>>, StringVecIter);

py_iterator!(OptionPropIter, Option<db_c::Prop>, PropValue);
py_iterator!(
    NestedOptionPropIter,
    BoxedIter<Option<db_c::Prop>>,
    OptionPropIter
);

py_iterator!(PropHistoryIter, Vec<(i64, db_c::Prop)>, PropHistory);
py_iterator!(
    NestedPropHistoryIter,
    BoxedIter<Vec<(i64, db_c::Prop)>>,
    PropHistoryIter
);

py_iterator!(PropsIter, HashMap<String, db_c::Prop>, Props);
py_iterator!(
    NestedPropsIter,
    BoxedIter<HashMap<String, db_c::Prop>>,
    PropsIter
);

py_iterator!(PropHistoriesIter, HashMap<String, Vec<(i64, db_c::Prop)>>, PropHistories);
py_iterator!(
    NestedPropHistoriesIter,
    BoxedIter<HashMap<String, Vec<(i64, db_c::Prop)>>>,
    PropHistoriesIter
);
