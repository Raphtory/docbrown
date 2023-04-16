use std::fmt::Display;
use std::ptr::addr_of;
// see http://lukaskalbertodt.github.io/2019/12/05/generalized-autoref-based-specialization.html
// https://github.com/dtolnay/case-studies/tree/master/autoref-specialization#realistic-application

macro_rules! repr {
    ($obj:expr) => {{
        #[allow(unused_imports)]
        use $crate::types::repr::Repr;
        (&&&ReprWrapper::new(&$obj)).repr()
    }};
}
