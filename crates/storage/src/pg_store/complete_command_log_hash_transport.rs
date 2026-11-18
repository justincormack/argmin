// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Request-bound scalar observations of complete retained commands. The hash
//! wire grammar is unchanged; only the selected durable reader decodes contexts.
//! Neither sparse observations nor a successful page prove a complete prefix.

use super::*;
use crate::storage_rpc::{
    decode_metadata_command_log_hash_range_response,
    encode_metadata_command_log_hash_range_response, StorageRpcMetadataCommandLogHashRangeResponse,
    StorageRpcPayloadError, STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES,
};

pub(in crate::node_runtime) struct CompleteLogHashRangeRequest<'a> {
    pg_id: PgId,
    epoch: ClusterEpoch,
    first: MetadataCommandLogIndex,
    last: MetadataCommandLogIndex,
    topology: &'a PgTopology,
}

/// Scope and actual covered interval survive even an empty or reduced read.
/// The scalar rows are observations, not recovery admission or retained custody.
#[derive(Debug)]
pub(in crate::node_runtime) struct CompleteLogHashPage {
    pg_id: PgId,
    epoch: ClusterEpoch,
    topology: PgTopology,
    first: MetadataCommandLogIndex,
    last: MetadataCommandLogIndex,
    entries: Vec<MetadataCommandLogHashRangeEntry>,
}

impl CompleteLogHashPage {
    pub(in crate::node_runtime) fn covered_range(
        &self,
    ) -> (MetadataCommandLogIndex, MetadataCommandLogIndex) {
        (self.first, self.last)
    }

    pub(in crate::node_runtime) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn entries(&self) -> &[MetadataCommandLogHashRangeEntry] {
        &self.entries
    }
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

impl<'a> CompleteLogHashRangeRequest<'a> {
    pub(in crate::node_runtime) fn new(
        pg_id: PgId,
        epoch: ClusterEpoch,
        first: MetadataCommandLogIndex,
        last: MetadataCommandLogIndex,
        topology: &'a PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        if !topology.contains_pg(pg_id)
            || last < first
            || last.get() - first.get() >= STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES
            || i64::try_from(last.get()).is_err()
            || i64::try_from(epoch.get()).is_err()
        {
            return Err(invalid("complete hash response request scope"));
        }
        Ok(Self {
            pg_id,
            epoch,
            first,
            last,
            topology,
        })
    }

    pub(in crate::node_runtime) fn read_response(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<Vec<u8>, CompleteLogReadError> {
        if pg.pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "read complete hash response PG",
            }
            .into());
        }
        let entries = pg.retained_complete_metadata_command_log_hashes(
            node_id,
            self.epoch,
            self.first,
            self.last,
            self.topology,
        )?;
        self.validate(&entries)?;
        Ok(encode_metadata_command_log_hash_range_response(
            &StorageRpcMetadataCommandLogHashRangeResponse { entries },
        )?)
    }

    pub(in crate::node_runtime) fn decode_page(
        &self,
        bytes: &[u8],
    ) -> Result<CompleteLogHashPage, StorageRpcPayloadError> {
        let response = decode_metadata_command_log_hash_range_response(bytes)?;
        self.validate(&response.entries)?;
        Ok(CompleteLogHashPage {
            pg_id: self.pg_id,
            epoch: self.epoch,
            topology: self.topology.clone(),
            first: self.first,
            last: self.last,
            entries: response.entries,
        })
    }

    fn validate(
        &self,
        entries: &[MetadataCommandLogHashRangeEntry],
    ) -> Result<(), StorageRpcPayloadError> {
        if entries.len() as u64 > self.last.get() - self.first.get() + 1 {
            return Err(invalid("complete hash response count exceeds request"));
        }
        for entry in entries {
            if entry.log_index < self.first.get() || entry.log_index > self.last.get() {
                return Err(invalid("complete hash response entry range"));
            }
            if entry.log_index == 1 && entry.previous_log_hash.value() != 0 {
                return Err(invalid("complete hash response genesis"));
            }
        }
        for pair in entries.windows(2) {
            if pair[0].log_index >= pair[1].log_index
                || (pair[0].log_index.checked_add(1) == Some(pair[1].log_index)
                    && pair[0].log_hash != pair[1].previous_log_hash)
            {
                return Err(invalid("complete hash response ordering/hash link"));
            }
        }
        Ok(())
    }
}
