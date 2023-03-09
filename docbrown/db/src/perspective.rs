use std::ops::Range;

#[derive(Debug, PartialEq)]
pub struct Perspective {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

impl Perspective {
    pub fn new(start: Option<i64>, end: Option<i64>) -> Perspective {
        Perspective {
            start, // inclusive
            end,   // exclusive
        }
    }
    pub fn range(start: i64, end: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            start: Some(start),
            end: Some(end),
            increment: increment as i64,
            window: None,
        }
    }
    pub fn walk(increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            start: None,
            end: None,
            increment: increment as i64,
            window: None,
        }
    }
    pub fn depart(start: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            start: Some(start),
            end: None,
            increment: increment as i64,
            window: None,
        }
    }
    pub fn climb(end: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            start: None,
            end: Some(end),
            increment: increment as i64,
            window: None,
        }
    }
    // TODO pub fn weeks(n), days(n), hours(n), minutes(n), seconds(n), millis(n)
}

#[derive(Clone)]
pub struct PerspectiveSet {
    start: Option<i64>,
    end: Option<i64>,
    increment: i64,
    window: Option<i64>,
}

impl PerspectiveSet {
    pub fn window(&self, size: u64) -> PerspectiveSet {
        PerspectiveSet {
            window: Some(size as i64),
            ..self.clone()
        }
    }
    pub(crate) fn build_iter(&self, timeline: Range<i64>) -> PerspectiveIterator {
        // TODO: alignment with the epoch for start
        let start = self.start.unwrap_or(timeline.start + self.increment);
        let end = self.end.unwrap_or(timeline.end);
        PerspectiveIterator {
            cursor: start,
            end: end,
            increment: self.increment,
            window: self.window,
        }
    }
}

pub(crate) struct PerspectiveIterator {
    cursor: i64,
    end: i64,
    increment: i64,
    window: Option<i64>,
}

impl Iterator for PerspectiveIterator {
    type Item = Perspective;
    fn next(&mut self) -> Option<Self::Item> {
        let limit = match self.window {
            Some(window) => self.cursor - window,
            None => self.cursor - self.increment,
        };
        if self.end <= limit {
            None
        } else {
            let current_cursor = self.cursor;
            self.cursor += self.increment;
            Some(Perspective {
                start: self.window.map(|w| current_cursor - w),
                end: Some(current_cursor),
            })
        }
    }
}

/*
only range, end/alignment, discrete windows, no slicing

NEXT STEPS:
 - walk, climb, depart (solve epoch alignment for walk and climb)
 - alignments
 - time vs discrete windows


FUNDAMENTAL DECISIONS:
 - A perspective should have the exact size set by the window, i.e. p.end - p.start = window
    - This means that either the start or the end of a perspective needs to be exclusive
       - [?] In the programming context in general, the start should be inclusive and not the end
       - [?] If we are using end-alignment, one might expect the end to be inclusive
 - The perspectives should be aligned with the start or with the end of the cursor position
    - [?] start/alignment makes sense for fixed size windows if we use autoalignment
    - [?] end/alignment makes sense for unbounded windows, i.e. looking to the past


API:
graph.range(start, end, increment).window(size) -> Iterator<Item=GraphView>
graph_view.perspectve.start
*/

#[cfg(test)]
mod perspective_tests {
    use itertools::Itertools;
    use crate::perspective::{Perspective, PerspectiveSet};

    #[test]
    fn perspective_range() {

        let range = Perspective::range(100, 200, 50);

        let expected = vec![
            Perspective::new(None, Some(100)),
            Perspective::new(None, Some(150)),
            Perspective::new(None, Some(200))];
        assert_eq!(range.build_iter(0..0).collect_vec(), expected);

        let windows = range.window(20);
        let expected = vec![
            Perspective::new(Some(81), Some(100)),
            Perspective::new(Some(131), Some(150)),
            Perspective::new(Some(181), Some(200))];
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);



        // let expected: Vec<Perspective> = vec![Perspective::new(Some(81), Some(100)), Perspective::new(Some(131), Some(150))];
        //
        //
        // for (current, expected) in windows.zip(expected) {//.foreach(|(current, expected)| assert_eq!(current, expected))
        //     assert_eq!(current, expected)
        // }
    }
}
