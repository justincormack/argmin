// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::needless_pass_by_value,
    clippy::struct_excessive_bools,
    clippy::too_many_lines,
    clippy::unreadable_literal,
    clippy::unused_self
)]

#[cfg(all(feature = "local-debug-endpoints", not(debug_assertions), not(test)))]
compile_error!(
    "local-debug-endpoints is only for test/debug builds and must not be enabled in release builds"
);

pub mod cors;
pub mod http;

pub(crate) use server_core::{conditional, coordinator, error, metadata_blob, range};
