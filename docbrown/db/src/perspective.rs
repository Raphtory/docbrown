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
            cursor: Some(start),
            end: Some(end),
            increment: increment as i64,
            window: None,
        }
    }

    pub fn walk(increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            cursor: None,
            end: None,
            increment: increment as i64,
            window: None,
        }
    }

    pub fn depart(start: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            cursor: Some(start),
            end: None,
            increment: increment as i64,
            window: None,
        }
    }

    pub fn climb(end: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            cursor: None,
            end: Some(end),
            increment: increment as i64,
            window: None,
        }
    }
}

#[derive(Clone)]
pub struct PerspectiveSet {
    cursor: Option<i64>, // the position of the cursor of this iterator, initially set to the start
    end: Option<i64>,
    increment: i64,
    window: Option<i64>,
}

impl PerspectiveSet {
    pub fn window(&self, size: u64) -> PerspectiveSet {
        PerspectiveSet {
            cursor: self.cursor,
            end: self.end,
            increment: self.increment,
            window: Some(size as i64), // TODO isnt there another syntax like {window, self..} ?
        }
    }
    pub fn set_timeline(self: &mut Self, timeline: Range<i64>) { // TODO this should probably return an iterator...
        if self.cursor.is_none() {
            self.cursor = Some(timeline.start + self.increment); // TODO: alignment
        }
        if self.end.is_none() {
            self.end = Some(timeline.end);
        }
    }
}

impl Iterator for PerspectiveSet {
    type Item = Perspective;
    fn next(&mut self) -> Option<Self::Item> {
        let limit = match self.window {
            Some(window) => self.cursor? - window,
            None => self.cursor? - self.increment,
        };
        if self.end? <= limit {
            None
        } else {
            let current_cursor = self.cursor?;
            self.cursor = Some(self.cursor? + self.increment);
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
    use crate::perspective::{Perspective, PerspectiveSet};

    #[test]
    fn perspective_range() {
        // let set = PerspectiveSet {
        //     cursor: 100,
        //     end: 160,
        //     increment: 50,
        //     window: Some(20),
        // }

        let mut range = Perspective::range(100, 160, 50);
        let windows = range.window(20);

        // let current: Vec<Perspective> = set.collect();
        // let expected: Vec<Perspective> = [Perspective::new(80, 100), Perspective::new(130, 150)].iter().collect();
        let expected: Vec<Perspective> = vec![Perspective::new(Some(81), Some(100)), Perspective::new(Some(131), Some(150))];
        // assert_eq!(current, expected)

        for (current, expected) in windows.zip(expected) {//.foreach(|(current, expected)| assert_eq!(current, expected))
            assert_eq!(current, expected)
        }
    }
}
