use polars::error::PolarsError;
use std::error::Error;
use std::fmt;
use std::fmt::Formatter;
use thiserror::__private::AsDynError;

#[derive(Debug)]
pub enum GraphError {
    StateSizeError,
    ExternalError(Box<dyn Error>),
    PolarsError(PolarsError),
}

pub type GraphResult<T> = Result<T, GraphError>;

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "invalid graph operation: {:?}", self)
    }
}

impl From<Box<dyn Error>> for GraphError {
    fn from(value: Box<dyn Error>) -> Self {
        GraphError::ExternalError(value)
    }
}

impl From<PolarsError> for GraphError {
    fn from(value: PolarsError) -> Self {
        GraphError::PolarsError(value)
    }
}

impl Error for GraphError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            GraphError::ExternalError(ref e) => Some(e.as_dyn_error()),
            GraphError::PolarsError(ref e) => Some(e),
            _ => None,
        }
    }
}
