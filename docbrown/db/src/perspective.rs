#[derive(Debug, PartialEq)]
pub struct Perspective {
    pub start: i64,
    pub end: i64,
}

impl Perspective {
    pub fn new(start: i64, end: i64) -> Perspective {
        Perspective {
            start, // inclusive // TODO these should be options!!
            end,   // exclusive
        }
    }

    pub fn range(start: i64, end: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            cursor: start,
            end: end,
            increment: increment as i64,
            window: None,
        }
    }
}

#[derive(Clone)]
pub struct PerspectiveSet {
    cursor: i64,
    end: i64,
    increment: i64,
    window: Option<i64>,
}

impl PerspectiveSet {
    pub fn back_windows(&mut self, size: u64) {
        self.window = Some(size as i64);
    }
    pub fn set_timeline(self: &mut Self, start: i64, end: i64) { // TODO this should probably return an iterator...
        self.cursor = start; // TODO override if it is not None or add additional param
        self.end = end;
    }
}

impl Iterator for PerspectiveSet {
    type Item = Perspective;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor > self.end {
            None
        } else {
            let current_cursor = self.cursor;
            self.cursor += self.increment;
            Some(Perspective {
                start: self.window.map_or_else(|| i64::MIN, |w| current_cursor - w + 1),
                end: current_cursor,
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
        range.back_windows(20);

        // let current: Vec<Perspective> = set.collect();
        // let expected: Vec<Perspective> = [Perspective::new(80, 100), Perspective::new(130, 150)].iter().collect();
        let expected: Vec<Perspective> = vec![Perspective::new(81, 100), Perspective::new(131, 150)];
        // assert_eq!(current, expected)

        for (current, expected) in range.zip(expected) {//.foreach(|(current, expected)| assert_eq!(current, expected))
            assert_eq!(current, expected)
        }
    }
}
