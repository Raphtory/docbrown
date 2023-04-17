use futures::AsyncReadExt;
use std::ops::Deref;
use std::ptr::addr_of;

pub trait Repr {
    fn repr(&self) -> String;
}

impl Repr for u32 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for u64 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for i32 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for i64 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for usize {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for f32 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for f64 {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for String {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl Repr for &str {
    fn repr(&self) -> String {
        self.to_string()
    }
}

impl<T: Repr + ?Sized> Repr for &T {
    fn repr(&self) -> String {
        self.repr()
    }
}

impl<T: Repr> Repr for Option<T> {
    fn repr(&self) -> String {
        match &self {
            Some(v) => v.repr(),
            None => "None".to_string(),
        }
    }
}

#[cfg(test)]
mod repr_tests {
    use super::*;

    struct ReprTester;

    impl Repr for ReprTester {
        fn repr(&self) -> String {
            "ReprTester".to_string()
        }
    }

    #[test]
    fn test_manual_definition() {
        let v = ReprTester;
        assert_eq!(v.repr(), "ReprTester")
    }

    #[test]
    fn test_nested_definition() {
        let v = Some(ReprTester);
        assert_eq!(v.repr(), "ReprTester")
    }

    #[test]
    fn test_option_no_macro() {
        let v = Some(1);

        assert_eq!(v.repr(), "1")
    }

    #[test]
    fn test_option_some() {
        let v = Some(1);

        assert_eq!(v.repr(), "1")
    }

    #[test]
    fn test_option_none() {
        let v: Option<String> = None;
        assert_eq!(v.repr(), "None")
    }

    #[test]
    fn test_int() {
        let v = 1;
        assert_eq!(v.repr(), "1")
    }

    #[test]
    fn test_int_ref() {
        let v = 1;
        assert_eq!((&v).repr(), "1")
    }

    fn test_string_ref() {
        let v = "test";

        assert_eq!(v.repr(), "test")
    }
}
