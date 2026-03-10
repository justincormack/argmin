#![allow(
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::match_same_arms,
    clippy::comparison_chain,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    clippy::unreadable_literal
)]

pub mod conditional;
pub mod coordinator;
pub mod error;
pub mod etag;
pub mod metadata_blob;
pub mod pg;
pub mod range;
