use super::*;
use crate::metadata_command::DeleteFinalizedBucketCommand;

include!("metadata/support.rs");
include!("metadata/command_apply.rs");
include!("metadata/command_mutations.rs");
include!("metadata/metadata_helpers.rs");
include!("metadata/row_decoders.rs");
include!("metadata/metadata_store.rs");
include!("metadata/direct_helpers.rs");

#[cfg(test)]
#[path = "metadata/tests.rs"]
mod tests;
