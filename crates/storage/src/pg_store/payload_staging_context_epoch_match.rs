// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact descriptive relationship for an epoch-transition proposal. Neither
//! source acceptance nor authority in the target epoch follows from equality.

use super::{CandidateBindingError, ContextError, ContextualCommand, Input};
use crate::PgTopology;

pub(super) fn require(
    candidate: &ContextualCommand,
    source: &ContextualCommand,
    topology: &PgTopology,
) -> Result<(), ContextError> {
    if topology != &candidate.topology || topology != &source.topology {
        return Err(CandidateBindingError::Topology.into());
    }
    let id = candidate.id();
    let source_id = source.id();
    if !topology.contains_pg(id.pg_id()) || !topology.contains_pg(source_id.pg_id()) {
        return Err(ContextError::CommandPg);
    }
    if id.pg_id() != source_id.pg_id() || id.cluster_epoch() <= source_id.cluster_epoch() {
        return Err(ContextError::EpochReissueContext);
    }
    // Exact payload and ORIGINAL context, not a same-PG or bare-envelope match.
    // New epochs may restart their log index at one. No reservation takeover,
    // abandoned-source substitution, descriptor recapture or cutoff renewal.
    let exact = match (&candidate.input, &source.input) {
        (Input::Ordinary(candidate), Input::Ordinary(source)) => {
            candidate.payload() == source.payload()
        }
        (Input::Described(candidate), Input::Described(source)) => {
            candidate.has_same_context(source)
                && candidate.command().payload() == source.command().payload()
        }
        (Input::AbandonedPart(candidate), Input::AbandonedPart(source)) => {
            candidate.has_same_context(source)
                && candidate.command().payload() == source.command().payload()
        }
        (Input::Ordinary(_), Input::Described(_) | Input::AbandonedPart(_))
        | (Input::Described(_), Input::Ordinary(_) | Input::AbandonedPart(_))
        | (Input::AbandonedPart(_), Input::Ordinary(_) | Input::Described(_)) => false,
    };
    if exact {
        Ok(())
    } else {
        Err(ContextError::EpochReissueContext)
    }
}
