pub /* PgId /* EcShape */ */ fn harmless_comment(_value: u64) {}
pub const HARMLESS_STRING: &str = "EcShape /* comment marker";
pub const HARMLESS_RAW_STRING: &str = r#"PgId // comment marker"#;
pub const HARMLESS_CHAR: char = '"';

pub struct BodyOnly;

impl Default for BodyOnly {
    fn default() -> Self {
        let _hidden: Option<PgId> = None;
        Self
    }
}
