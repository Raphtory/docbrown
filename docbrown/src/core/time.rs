use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, ParseError};
use itertools::{Either, Itertools};
use regex::Regex;
use std::num::ParseIntError;
use std::ops::{Add, Sub};

pub trait IntoTime {
    fn into_time(&self) -> Result<i64, ParseTimeError>;
    // fn epoch_alignment() -> bool;
}

impl IntoTime for i64 {
    fn into_time(&self) -> Result<i64, ParseTimeError> {
        Ok(*self)
    }
}

impl IntoTime for &str {
    fn into_time(&self) -> Result<i64, ParseTimeError> {
        Ok(DateTime::parse_from_rfc3339(self)?.timestamp_millis())
    }
}

pub(crate) trait IntoTimeWithFormat {
    fn parse_time(&self, fmt: &str) -> Result<i64, ParseTimeError>;
}

impl IntoTimeWithFormat for &str {
    fn parse_time(&self, fmt: &str) -> Result<i64, ParseTimeError> {
        Ok(DateTime::parse_from_str(self, fmt)?.timestamp_millis())
    }
}

pub(crate) trait IntoBoundWithFormat {
    fn into_bound(&self, format: Option<&str>) -> Result<i64, ParseTimeError>;
}

impl IntoBoundWithFormat for &str {
    fn into_bound(&self, format: Option<&str>) -> Result<i64, ParseTimeError> {
        let date_format = format.unwrap_or("%Y-%m-%d");
        let date_result = NaiveDate::parse_from_str(self, date_format);

        let datetime = match date_result {
            Ok(date) => date.and_hms_milli_opt(23, 59, 59, 999).unwrap(),
            Err(date_err) => {
                let datetime_format = format.unwrap_or("%Y-%m-%d %H:%M:%S"); // TODO: add millis
                NaiveDateTime::parse_from_str(self, datetime_format).map_err(|datetime_err| {
                    ParseTimeError::InvalidBound(self.to_string(), date_err, datetime_err)
                })?
            }
        };
        Ok(datetime.timestamp_millis())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Interval {
    Discrete(u64),
    Temporal(Duration),
    // Calendar(Duration, Months, Years), // TODO
}

impl TryFrom<&str> for Interval {
    type Error = ParseTimeError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let trimmed = value.trim();
        let no_and = trimmed.replace("and", "");
        let cleaned = {
            let re = Regex::new(r"[\s&,]+").unwrap();
            re.replace_all(&no_and, " ")
        };

        let tokens = cleaned.split(" ").collect_vec();

        if tokens.len() < 2 || tokens.len() % 2 != 0 {
            return Err(ParseTimeError::InvalidPairs);
        }

        let (durations, errors): (Vec<Duration>, Vec<ParseTimeError>) = tokens
            .chunks(2)
            .map(|chunk| Self::parse_duration(chunk[0], chunk[1]))
            .partition_map(|d| match d {
                Ok(d) => Either::Left(d),
                Err(e) => Either::Right(e),
            });

        if errors.is_empty() {
            Ok(Interval::Temporal(
                durations.into_iter().reduce(|a, b| a + b).unwrap(),
            ))
        } else {
            Err(errors.get(0).unwrap().clone())
        }
    }
}

impl From<u64> for Interval {
    fn from(value: u64) -> Self {
        Self::Discrete(value)
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ParseTimeError {
    #[error("the interval string doesn't contain a complete number of number-unit pairs")]
    InvalidPairs,
    #[error("one of the tokens in the interval string supposed to be a number couldn't be parsed")]
    ParseInt {
        #[from]
        source: ParseIntError,
    },
    #[error("'{0}' is not a valid unit")]
    InvalidUnit(String),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error("bound couldn't be parse as date because {0} or as datetime because {1}")]
    InvalidBound(String, ParseError, ParseError),
}

impl Interval {
    /// Return an option because there might be no exact translation to millis for some intervals
    pub(crate) fn to_millis(&self) -> Option<i64> {
        // TODO: maybe should be u64
        match self {
            Self::Discrete(millis) => Some(*millis as i64),
            Self::Temporal(duration) => Some(duration.num_milliseconds()),
        }
    }

    fn parse_duration(number: &str, unit: &str) -> Result<Duration, ParseTimeError> {
        let number: i64 = number.parse::<u64>()? as i64;
        let duration = match unit {
            "week" | "weeks" => Duration::weeks(number),
            "day" | "days" => Duration::days(number),
            "hour" | "hours" => Duration::hours(number),
            "minute" | "minutes" => Duration::minutes(number),
            "second" | "seconds" => Duration::seconds(number),
            "millisecond" | "milliseconds" => Duration::milliseconds(number),
            unit => return Err(ParseTimeError::InvalidUnit(unit.to_string())),
        };
        Ok(duration)
    }
}

impl Sub<Interval> for i64 {
    type Output = i64;
    fn sub(self, rhs: Interval) -> Self::Output {
        match rhs {
            Interval::Discrete(number) => self - (number as i64),
            Interval::Temporal(duration) => self - duration.num_milliseconds(),
        }
    }
}

impl Add<Interval> for i64 {
    type Output = i64;
    fn add(self, rhs: Interval) -> Self::Output {
        match rhs {
            Interval::Discrete(number) => self + (number as i64),
            Interval::Temporal(duration) => self + duration.num_milliseconds(),
        }
    }
}

#[cfg(test)]
mod perspective_tests {
    use crate::core::time::{Interval, ParseTimeError};
    #[test]
    fn interval_parsing() {
        let second: i64 = 1000;
        let minute = 60 * second;
        let hour = 60 * minute;
        let day = 24 * hour;
        let week = 7 * day;

        let interval: Interval = "1 day".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), day);

        let interval: Interval = "1 week".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), week);

        let interval: Interval = "4 weeks and 1 day".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), 4 * week + day);

        let interval: Interval = "2 days & 1 millisecond".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), 2 * day + 1);

        let interval: Interval = "2 days, 1 hour, and 2 minutes".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), 2 * day + hour + 2 * minute);

        let interval: Interval = "1 weeks ,   1 minute".try_into().unwrap();
        assert_eq!(interval.to_millis().unwrap(), week + minute);

        let interval: Interval = "23 seconds  and 34 millisecond and 1 minute"
            .try_into()
            .unwrap();
        assert_eq!(interval.to_millis().unwrap(), 23 * second + 34 + minute);
    }

    #[test]
    fn invalid_intervals() {
        let result: Result<Interval, ParseTimeError> = "".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidPairs));

        let result: Result<Interval, ParseTimeError> = "1".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidPairs));

        let result: Result<Interval, ParseTimeError> = "1 day and 5".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidPairs));

        let result: Result<Interval, ParseTimeError> = "1 daay".try_into();
        assert_eq!(result, Err(ParseTimeError::InvalidUnit("daay".to_string())));
    }
}
