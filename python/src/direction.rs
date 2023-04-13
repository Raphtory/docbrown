use docbrown::core::Direction;
use pyo3::pyclass;
use std::fmt;

#[pyclass(name = "Direction")]
#[derive(Clone)]
pub enum PyDirection {
    BOTH,
    IN,
    OUT,
}

impl fmt::Display for PyDirection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PyDirection::BOTH => write!(f, "BOTH"),
            PyDirection::IN => write!(f, "IN"),
            PyDirection::OUT => write!(f, "OUT"),
        }
    }
}

impl From<PyDirection> for Direction {
    fn from(direction: PyDirection) -> Direction {
        // implement match for different values of str for in both and out
        match direction {
            PyDirection::BOTH => Direction::BOTH,
            PyDirection::IN => Direction::IN,
            PyDirection::OUT => Direction::OUT,
        }
    }
}

impl From<Direction> for PyDirection {
    fn from(direction: Direction) -> PyDirection {
        match direction {
            Direction::BOTH => PyDirection::BOTH,
            Direction::IN => PyDirection::IN,
            Direction::OUT => PyDirection::OUT,
        }
    }
}
