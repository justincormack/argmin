// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::DeleteFinalizedBucketCommand;

#[path = "metadata/segment_vid_window.rs"]
mod segment_vid_window;

#[path = "metadata/bucket_reservation_acquire.rs"]
mod bucket_reservation_acquire;

#[path = "metadata/bucket_drain_begin.rs"]
mod bucket_drain_begin;

#[path = "metadata/reservation_heartbeat.rs"]
mod reservation_heartbeat;
#[cfg(test)]
#[path = "metadata/reservation_release_tests.rs"]
mod reservation_release_tests;
#[cfg(test)]
pub(crate) use reservation_heartbeat::HeartbeatEffect;

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
