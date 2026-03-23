use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Number(value as i64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Number(value as i64)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Self::Number(value as i64)
    }
}

#[macro_export]
macro_rules! json {
    (null) => {
        $crate::Value::Null
    };
    ({ $($key:literal : $value:tt),* $(,)? }) => {{
        let mut object = ::std::collections::BTreeMap::new();
        $(
            object.insert($key.to_string(), $crate::json!($value));
        )*
        $crate::Value::Object(object)
    }};
    ([ $($value:tt),* $(,)? ]) => {{
        $crate::Value::Array(vec![$($crate::json!($value)),*])
    }};
    ($other:expr) => {
        $crate::Value::from($other)
    };
}

