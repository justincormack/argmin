pub
type Renamed = EcShape;

pub
use crate::hidden::*;

pub
fn expose(_: Renamed) {}

pub trait Boundary {
    fn
    expose(&self, value: EcShape);
}

pub /* gap */ type CommentRenamed = EcShape;
pub /* gap */ use crate::comment_hidden::*;
pub /* gap */ fn expose_comment_alias(_: CommentRenamed) {}

pub /* PgId /* EcShape */ */ fn harmless_comment(_value: u64) {}
pub const HARMLESS_STRING: &str = "EcShape /* comment marker";
pub const HARMLESS_RAW_STRING: &str = r#"PgId // comment marker"#;
pub const HARMLESS_CHAR: char = '"';

pub union Exposed {
    pub pg: PgId,
}

pub struct Opaque;

impl Iterator for Opaque {
    type Item = PgId;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl From<EcShape> for Opaque {
    fn from(_: EcShape) -> Self { Self }
}

pub struct BodyOnly;

impl Default for BodyOnly {
    fn default() -> Self { let _hidden: Option<PgId> = None; Self }
}
