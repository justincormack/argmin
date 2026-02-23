use smallvec::SmallVec;
use thiserror::Error;

/// A topology level tag — identifies which axis of the hierarchy a segment refers to.
///
/// Implemented as a newtype over u8 rather than an enum, so callers can define their
/// own levels without changing this crate:
///
///   const DATACENTER: Level = Level(8);   // coarser than Zone
///   const ROW:        Level = Level(24);  // between Zone and Rack
///   const PDU:        Level = Level(40);  // between Rack and Machine
///
/// Built-in constants are spaced at multiples of 16, leaving 15 values between each
/// pair for caller-defined levels.
///
/// Ord on the inner u8 means broader levels (lower values) sort before narrower ones,
/// consistent with TopologyKey segment ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Level(pub u8);

impl Level {
    pub const ZONE:    Level = Level(16);
    pub const RACK:    Level = Level(32);
    pub const MACHINE: Level = Level(48);
    pub const DISK:    Level = Level(64);
}

/// The physical location of a node, as an ordered list of (Level, id) segments.
///
/// Segments must be ordered from broadest to most specific and must not repeat a
/// Level. `TopologyKey::new` enforces this: it sorts by Level and returns
/// `Err(DuplicateLevel)` if any Level appears more than once.
///
/// Inline storage for up to 4 segments; no heap allocation for the common case.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopologyKey(SmallVec<[(Level, u32); 4]>);

impl TopologyKey {
    /// Construct from a slice of (Level, id) segments.
    ///
    /// Segments are sorted by Level automatically.
    /// Returns Err(DuplicateLevel) if any Level appears more than once.
    pub fn new(segments: &[(Level, u32)]) -> Result<Self, TopologyError> {
        let mut v: SmallVec<[(Level, u32); 4]> = SmallVec::from_slice(segments);
        v.sort_unstable_by_key(|&(level, _)| level);
        for w in v.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(TopologyError::DuplicateLevel(w[0].0));
            }
        }
        Ok(TopologyKey(v))
    }

    /// Convenience: single RACK segment.
    pub fn rack(rack: u32) -> Self {
        TopologyKey(smallvec::smallvec![(Level::RACK, rack)])
    }

    /// Convenience: RACK + MACHINE segments (common two-level case).
    pub fn rack_machine(rack: u32, machine: u32) -> Self {
        // RACK < MACHINE so order is already correct
        TopologyKey(smallvec::smallvec![
            (Level::RACK, rack),
            (Level::MACHINE, machine)
        ])
    }

    /// Read-only view of the segments, in Level order.
    pub fn segments(&self) -> &[(Level, u32)] {
        &self.0
    }

    /// Return the value for a given level, if present.
    pub fn level(&self, kind: Level) -> Option<u32> {
        self.0.iter().find(|&&(l, _)| l == kind).map(|&(_, v)| v)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TopologyError {
    #[error("level {0:?} appears more than once in TopologyKey")]
    DuplicateLevel(Level),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_constants() {
        assert!(Level::ZONE < Level::RACK);
        assert!(Level::RACK < Level::MACHINE);
        assert!(Level::MACHINE < Level::DISK);
        assert_eq!(Level::ZONE.0, 16);
        assert_eq!(Level::RACK.0, 32);
        assert_eq!(Level::MACHINE.0, 48);
        assert_eq!(Level::DISK.0, 64);
    }

    #[test]
    fn custom_level_between_builtins() {
        const ROW: Level = Level(24); // between ZONE and RACK
        assert!(Level::ZONE < ROW);
        assert!(ROW < Level::RACK);
    }

    #[test]
    fn topology_key_rack() {
        let k = TopologyKey::rack(3);
        assert_eq!(k.level(Level::RACK), Some(3));
        assert_eq!(k.level(Level::ZONE), None);
        assert_eq!(k.segments(), &[(Level::RACK, 3)]);
    }

    #[test]
    fn topology_key_rack_machine() {
        let k = TopologyKey::rack_machine(3, 7);
        assert_eq!(k.level(Level::RACK), Some(3));
        assert_eq!(k.level(Level::MACHINE), Some(7));
        assert_eq!(k.segments(), &[(Level::RACK, 3), (Level::MACHINE, 7)]);
    }

    #[test]
    fn topology_key_new_sorts() {
        let k = TopologyKey::new(&[(Level::MACHINE, 7), (Level::RACK, 3)]).unwrap();
        assert_eq!(k.segments(), &[(Level::RACK, 3), (Level::MACHINE, 7)]);
    }

    #[test]
    fn topology_key_new_empty() {
        let k = TopologyKey::new(&[]).unwrap();
        assert_eq!(k.segments(), &[]);
        assert_eq!(k.level(Level::RACK), None);
    }

    #[test]
    fn topology_key_duplicate_level() {
        let err = TopologyKey::new(&[(Level::RACK, 1), (Level::RACK, 2)]).unwrap_err();
        assert_eq!(err, TopologyError::DuplicateLevel(Level::RACK));
    }

    #[test]
    fn topology_key_custom_level() {
        const MY_LEVEL: Level = Level(24);
        let k = TopologyKey::new(&[(MY_LEVEL, 5)]).unwrap();
        assert_eq!(k.level(MY_LEVEL), Some(5));
        assert_eq!(k.level(Level::RACK), None);
    }

    #[test]
    fn topology_key_equality_order_independent() {
        let a = TopologyKey::new(&[(Level::MACHINE, 7), (Level::RACK, 3)]).unwrap();
        let b = TopologyKey::new(&[(Level::RACK, 3), (Level::MACHINE, 7)]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn topology_key_zone_rack_machine_disk() {
        let k = TopologyKey::new(&[
            (Level::DISK, 0),
            (Level::ZONE, 1),
            (Level::MACHINE, 7),
            (Level::RACK, 3),
        ])
        .unwrap();
        // Should come out sorted: ZONE, RACK, MACHINE, DISK
        assert_eq!(
            k.segments(),
            &[
                (Level::ZONE, 1),
                (Level::RACK, 3),
                (Level::MACHINE, 7),
                (Level::DISK, 0),
            ]
        );
    }
}
