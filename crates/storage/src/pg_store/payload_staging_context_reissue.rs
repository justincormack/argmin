// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Preserve the complete decoded variant across a descriptive reissue. This
//! neither replaces a pending slot nor forgets an installation observation:
//! owning submission/recovery holders are a separate boundary. The mandatory
//! authenticated carrier/custody switch has not been activated.

use super::{ContextError, ContextualCommand, Input};
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandId};

pub(in super::super::super) struct ContextReissueFailure {
    source: Box<ContextualCommand>,
    error: ContextError,
}

impl std::fmt::Debug for ContextReissueFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextReissueFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl ContextReissueFailure {
    pub(in super::super::super) fn error(&self) -> &ContextError {
        &self.error
    }

    /// Recover the same complete input, not its raw command or a fresh ordinary
    /// context. No borrowed owning value or mutable/split accessor is available.
    pub(in super::super::super) fn into_context(self) -> ContextualCommand {
        *self.source
    }
}

pub(super) fn derive(
    source: ContextualCommand,
    id: MetadataCommandId,
) -> Result<ContextualCommand, ContextReissueFailure> {
    match derive_input(&source, id) {
        Ok(input) => Ok(ContextualCommand {
            input,
            topology: source.topology,
        }),
        Err(error) => Err(ContextReissueFailure {
            source: Box::new(source),
            error,
        }),
    }
}

/// Consuming input to the submission holder's reissue transition. Retain BOTH
/// complete descriptions; the holder, not this descriptive layer, carries the
/// source installation history. Ordinary pending replacement is same-epoch.
pub(super) fn derive_same_epoch_pair(
    source: ContextualCommand,
    id: MetadataCommandId,
) -> Result<(ContextualCommand, ContextualCommand), ContextReissueFailure> {
    derive_pair(source, id, PairKind::SameEpoch)
}

pub(super) fn derive_epoch_pair(
    source: ContextualCommand,
    id: MetadataCommandId,
) -> Result<(ContextualCommand, ContextualCommand), ContextReissueFailure> {
    derive_pair(source, id, PairKind::NewEpoch)
}

enum PairKind {
    SameEpoch,
    NewEpoch,
}

fn derive_pair(
    source: ContextualCommand,
    id: MetadataCommandId,
    kind: PairKind,
) -> Result<(ContextualCommand, ContextualCommand), ContextReissueFailure> {
    let epoch_matches = match kind {
        PairKind::SameEpoch => source.id().cluster_epoch() == id.cluster_epoch(),
        PairKind::NewEpoch => source.id().cluster_epoch() < id.cluster_epoch(),
    };
    let derived = if epoch_matches {
        derive_input(&source, id)
    } else {
        Err(ContextError::ReissuePosition)
    };
    match derived {
        Ok(input) => {
            let successor = ContextualCommand {
                input,
                topology: source.topology.clone(),
            };
            Ok((source, successor))
        }
        Err(error) => Err(ContextReissueFailure {
            source: Box::new(source),
            error,
        }),
    }
}

// Private to the transformation implementation. No borrowed full-context
// successor API is exposed to submission/reissue holders or their callers.
fn derive_input(source: &ContextualCommand, id: MetadataCommandId) -> Result<Input, ContextError> {
    if !super::super::command_follows(source.id(), id) {
        return Err(ContextError::ReissuePosition);
    }
    // The original context is the sole selector. No raw envelope escapes, no
    // caller supplies replacement metadata, and no time/route recapture occurs.
    match &source.input {
        Input::Ordinary(command) => Ok(Input::Ordinary(MetadataCommandEnvelope::new(
            id,
            command.payload().clone(),
        ))),
        Input::Described(candidate) => candidate
            .reissued_at(id)
            .map(Input::Described)
            .map_err(ContextError::from),
        Input::AbandonedPart(candidate) => candidate
            .reissued_at(id)
            .map(Input::AbandonedPart)
            .map_err(ContextError::from),
    }
}
