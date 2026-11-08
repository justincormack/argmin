// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Match the complete pending description, never the bare envelope alone.
//! This read-only result grants no installation or recovery authority. The
//! coordinated carrier switch must use it inside the pending transaction;
//! current v13 command bytes cannot authenticate a separately supplied context.

use super::{CandidateBindingError, ContextError, ContextualCommand, Input};
use crate::PgTopology;

#[derive(Debug, PartialEq, Eq)]
pub(in super::super::super) enum PendingContextMatch {
    ExactRetry,
    SameEpochReissue,
    Conflict,
}

pub(super) fn classify(
    candidate: &ContextualCommand,
    source: &ContextualCommand,
    topology: &PgTopology,
) -> Result<PendingContextMatch, ContextError> {
    use PendingContextMatch::{Conflict, ExactRetry, SameEpochReissue};

    if topology != &candidate.topology || topology != &source.topology {
        return Err(CandidateBindingError::Topology.into());
    }
    if !topology.contains_pg(candidate.id().pg_id()) || !topology.contains_pg(source.id().pg_id()) {
        return Err(ContextError::CommandPg);
    }
    let (candidate_command, source_command, ordinary) = match (&candidate.input, &source.input) {
        (Input::Ordinary(candidate), Input::Ordinary(source)) => (candidate, source, true),
        (Input::Described(candidate), Input::Described(source)) => {
            if !candidate.has_same_context(source) {
                return Ok(Conflict);
            }
            (candidate.command(), source.command(), false)
        }
        (Input::AbandonedPart(candidate), Input::AbandonedPart(source)) => {
            if !candidate.has_same_context(source) {
                return Ok(Conflict);
            }
            (candidate.command(), source.command(), false)
        }
        (Input::Ordinary(_), Input::Described(_) | Input::AbandonedPart(_))
        | (Input::Described(_), Input::Ordinary(_) | Input::AbandonedPart(_))
        | (Input::AbandonedPart(_), Input::Ordinary(_) | Input::Described(_)) => {
            return Ok(Conflict)
        }
    };
    if candidate_command == source_command {
        return Ok(ExactRetry);
    }
    let id = candidate_command.id();
    let source_id = source_command.id();
    if id.pg_id() != source_id.pg_id()
        || id.cluster_epoch() != source_id.cluster_epoch()
        || id.log_index() <= source_id.log_index()
    {
        return Ok(Conflict);
    }
    // Preserve the existing CommitMultipartObject reservation-takeover rule
    // only for ordinary commands. Described/cleanup commands require the exact
    // payload AND original context; cross-epoch recovery is a separate protocol.
    let payload_matches = if ordinary {
        candidate_command
            .payload()
            .is_ordinary_pending_slot_reissue_of(source_command.payload())
    } else {
        candidate_command.payload() == source_command.payload()
    };
    Ok(if payload_matches {
        SameEpochReissue
    } else {
        Conflict
    })
}
