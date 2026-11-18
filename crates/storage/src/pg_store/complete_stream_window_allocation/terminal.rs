// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Finish the frontend file-attempt phase only after every original member has
//! confirmed the exact append. Durable session/owner custody remains untouched.
use super::*;

#[path = "remote_terminal.rs"]
mod remote;
pub(in crate::node_runtime) use remote::RemoteStreamAppendTerminal;

#[must_use]
pub(in crate::node_runtime) struct StreamAppendTerminalFailure<'route> {
    publication: Box<AcknowledgedStreamAppendPublication<'route>>,
    error: FanoutError,
}

impl<'route> StreamAppendTerminalFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &FanoutError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> AcknowledgedStreamAppendPublication<'route> {
        *self.publication
    }
}

impl<'route> AcknowledgedStreamAppendPublication<'route> {
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<CompleteTerminalHandoff<'route>, StreamAppendTerminalFailure<'route>> {
        self.into_terminal_parts(false)
            .map(|(terminal, _)| terminal)
    }

    pub(in crate::node_runtime) fn into_remote_terminal(
        self,
    ) -> Result<RemoteStreamAppendTerminal<'route>, StreamAppendTerminalFailure<'route>> {
        self.into_terminal_parts(true).map(|(terminal, identity)| {
            RemoteStreamAppendTerminal::new(terminal, identity.expect("checked remote session"))
        })
    }

    fn into_terminal_parts(
        self,
        remote: bool,
    ) -> Result<
        (
            CompleteTerminalHandoff<'route>,
            Option<crate::node_client::MetadataCommandSessionIdentity>,
        ),
        StreamAppendTerminalFailure<'route>,
    > {
        // Every fallible check precedes any ownership split. Failure retains
        // original physical attempts, complete source, all journals and cutoffs.
        let result = (|| {
            if self.remote_session.is_some() != remote {
                return Err(Error::Window(
                    "remote append terminal handoff requires its matching session-bound holder"
                        .into(),
                )
                .into());
            }
            let writer = self.custody.writer();
            writer.allocated.window.require_valid()?;
            writer.fence.require_valid_for(self.id().cluster_epoch())?;
            let PublicationSource::Submitted { deadline, .. } = &self.sender.source else {
                unreachable!()
            };
            let cutoff = deadline
                .0
                .get()
                .expect("append retains first publication cutoff");
            if Instant::now() >= cutoff.local
                || crate::clock::current_time_millis()
                    >= cutoff.portable.portable_wall_valid_until_ms
            {
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "append terminal original publication cutoff",
                }
                .into());
            }
            let route = &writer.allocated.window.terminal.owner.route;
            require_cohort_completion(route, &self.sender)?;
            terminal_work_for_handoff(route, &self.sender)
        })();
        let (work, slot_work) = match result {
            Ok(work) => work,
            Err(error) => {
                return Err(StreamAppendTerminalFailure {
                    publication: Box::new(self),
                    error,
                })
            }
        };
        let Self {
            custody,
            sender,
            remote_session,
        } = self;
        let writer = match custody.custody {
            Custody::Embedded(writer) => writer,
            Custody::Remote(remote) => remote.writer,
        };
        // All-member apply has installed exact staged references and successor
        // owner windows. The prior window's slot was retired before file binding.
        // Retain the SAME admission/cohort and new command history; discard only
        // resolved frontend receipts/old source bookkeeping, never durable state.
        // This also prevents successive appends retaining a recursive file history.
        let route = writer.allocated.window.terminal.owner.route;
        Ok((
            CompleteTerminalHandoff::from_confirmed_owner(
                CompleteFanoutSender { route, sender },
                work,
                slot_work,
            ),
            remote_session,
        ))
    }
}
