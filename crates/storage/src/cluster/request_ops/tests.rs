#[cfg(test)]
mod local_diagnostic_tests {
    use super::*;

    #[test]
    fn metadata_checkpoint_diagnostic_is_exact_and_storage_owned() {
        let summary = MetadataCommandCheckpointRecordSummary {
            scanned: 1,
            recorded: 2,
            already_current: 3,
            skipped_cadence: 4,
            skipped_inactive: 5,
            skipped_empty: 6,
            skipped_stale_epoch: 7,
            compacted: 8,
            compaction_deleted_entries: 9,
            compaction_noop: 10,
            compaction_no_checkpoint: 11,
            compaction_pending: 12,
            compaction_failed: 0,
            failed: 0,
            limit_reached: true,
        };

        let diagnostic = metadata_checkpoint_diagnostic_from_result(PgId::new(13), Ok(summary));
        assert_eq!(
            diagnostic.outcome(),
            MetadataCheckpointDiagnosticOutcome::Success
        );
        assert_eq!(
            diagnostic.into_text(),
            concat!(
                "pg_id=13 scanned=1 recorded=2 already_current=3 skipped_cadence=4 ",
                "skipped_inactive=5 skipped_empty=6 skipped_stale_epoch=7 compacted=8 ",
                "compaction_deleted_entries=9 compaction_noop=10 compaction_no_checkpoint=11 ",
                "compaction_pending=12 compaction_failed=0 failed=0 limit_reached=true\n",
            )
        );
    }

    #[test]
    fn metadata_checkpoint_selector_grammar_is_storage_owned() {
        assert_eq!(
            parse_metadata_checkpoint_selector("13").unwrap(),
            PgId::new(13)
        );

        let diagnostic = parse_metadata_checkpoint_selector("not-a-number").unwrap_err();
        assert_eq!(
            diagnostic.outcome(),
            MetadataCheckpointDiagnosticOutcome::InvalidInput
        );
        assert_eq!(diagnostic.into_text(), "invalid pg id\n");
    }

    #[test]
    fn local_diagnostic_failures_and_debug_are_redacted() {
        let checkpoint = metadata_checkpoint_diagnostic_from_result(
            PgId::new(17),
            Err(StoreError::Io {
                context: "secret checkpoint path",
                source: std::io::Error::other("secret checkpoint source"),
            }),
        );
        assert_eq!(
            checkpoint.outcome(),
            MetadataCheckpointDiagnosticOutcome::Conflict
        );
        let checkpoint_debug = format!("{checkpoint:?}");
        let checkpoint_text = checkpoint.into_text();
        assert_eq!(
            checkpoint_text,
            "pg_id=17 checkpoint_record_failed=store_io_failure\n"
        );
        assert!(!checkpoint_debug.contains("secret"));
        assert!(!checkpoint_text.contains("secret"));

        let placement =
            object_payload_placement_failure(ObjectPgActionError::Store(StoreError::Io {
                context: "secret placement path",
                source: std::io::Error::other("secret placement source"),
            }));
        assert_eq!(
            placement.outcome(),
            ObjectPayloadPlacementDiagnosticOutcome::Conflict
        );
        let placement_debug = format!("{placement:?}");
        let placement_text = placement.into_text();
        assert_eq!(
            placement_text,
            "object payload placement unavailable: store_io_failure\n"
        );
        assert!(!placement_debug.contains("secret"));
        assert!(!placement_text.contains("secret"));
    }
}

#[cfg(test)]
mod bounded_pg_scan_tests {
    use super::bounded_pg_scan_window;

    #[test]
    fn bounded_pg_scan_visits_200_pgs_once_in_linear_batches() {
        let pg_ids = (0..200).collect::<Vec<_>>();
        let mut visited = Vec::new();
        let mut next_pg_id = None;
        let mut batches = 0usize;

        loop {
            let window = bounded_pg_scan_window(&pg_ids, next_pg_id, 8);
            visited.extend_from_slice(&pg_ids[window.start..window.end]);
            batches += 1;
            next_pg_id = window.next_pg_id;
            if next_pg_id.is_none() {
                break;
            }
        }

        assert_eq!(visited, pg_ids);
        assert_eq!(batches, 25);
    }
}

#[cfg(test)]
mod pending_command_terminal_cleanup_tests {
    use super::*;

    fn remote_failure(code: StorageRpcErrorCode) -> BucketSnapshotLoadError {
        StoreError::StorageRpc {
            node_id: 7,
            operation: "test metadata command apply",
            failure: code,
            detail: crate::error::StorageNodeFailureDetail::new("test remote failure"),
        }
        .into()
    }

    #[test]
    fn metadata_command_apply_retries_only_remote_transient_failures() {
        assert!(metadata_command_apply_transport_error_is_retryable(
            &remote_failure(StorageRpcErrorCode::TransportTimeout)
        ));
        assert!(metadata_command_apply_transport_error_is_retryable(
            &remote_failure(StorageRpcErrorCode::TransportClosed)
        ));
        assert!(metadata_command_apply_transport_error_is_retryable(
            &remote_failure(StorageRpcErrorCode::MetadataCommandContention)
        ));
        assert!(!metadata_command_apply_transport_error_is_retryable(
            &StoreError::Io {
                context: "test local storage failure",
                source: std::io::Error::other("test local storage failure"),
            }
            .into()
        ));
    }

    #[test]
    fn applied_command_release_defers_metadata_contention_but_not_invariants() {
        assert!(applied_metadata_command_cleanup_error_is_retryable(
            &BucketSnapshotLoadError::Metadata(
                MetadataError::BucketWriteReservationConflict {
                    reservation_id: "reservation".to_string(),
                },
            )
        ));
        assert!(!applied_metadata_command_cleanup_error_is_retryable(
            &BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                context: "test terminal cleanup",
                reason: "test invariant".to_string(),
            })
        ));
    }

    #[test]
    fn pending_slot_remove_retries_response_loss_after_remote_removal() {
        let mut pending = true;
        let mut attempts = 0usize;
        let mut work_budget =
            super::super::RequestWorkBudget::new(std::time::Duration::from_secs(1), None);

        let cleanup = remove_pending_metadata_command_slot_after_terminal_outcome(
            PgId::new(7),
            Some(&mut work_budget),
            || {
                attempts += 1;
                if attempts == 1 {
                    pending = false;
                    return Err(StoreError::Io {
                        context: "read storage RPC response",
                        source: std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "injected response loss after pending-slot removal",
                        ),
                    });
                }
                Ok(std::mem::replace(&mut pending, false))
            },
        )
        .unwrap();

        assert_eq!(
            cleanup,
            PendingMetadataCommandTerminalCleanup::AlreadyAbsent,
            "the retry should confirm the slot is already absent",
        );
        assert_eq!(attempts, 2);
    }

    #[test]
    fn pending_slot_remove_budget_exhaustion_preserves_committed_outcome() {
        let mut pending = true;
        let mut attempts = 0usize;
        let mut work_budget =
            super::super::RequestWorkBudget::new(std::time::Duration::from_secs(1), Some(1));

        let cleanup = remove_pending_metadata_command_slot_after_terminal_outcome(
            PgId::new(7),
            Some(&mut work_budget),
            || {
                attempts += 1;
                pending = false;
                Err(StoreError::Io {
                    context: "read storage RPC response",
                    source: std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "injected response loss after pending-slot removal",
                    ),
                })
            },
        )
        .unwrap();

        assert_eq!(cleanup, PendingMetadataCommandTerminalCleanup::Deferred);
        assert!(
            !pending,
            "the remote cleanup committed before response loss"
        );
        assert_eq!(attempts, 1);
    }

    #[test]
    fn pending_slot_remove_uses_callers_existing_budget() {
        let mut attempts = 0usize;
        let mut work_budget =
            super::super::RequestWorkBudget::new(std::time::Duration::from_secs(1), Some(1));
        work_budget.check("consume caller budget").unwrap();

        let cleanup = remove_pending_metadata_command_slot_after_terminal_outcome(
            PgId::new(7),
            Some(&mut work_budget),
            || {
                attempts += 1;
                Ok(true)
            },
        )
        .unwrap();

        assert_eq!(cleanup, PendingMetadataCommandTerminalCleanup::Deferred);
        assert_eq!(attempts, 0, "cleanup must not reset the caller's budget");
    }

    #[test]
    fn pending_slot_remove_does_not_retry_semantic_failure() {
        let mut attempts = 0usize;
        let error =
            remove_pending_metadata_command_slot_after_terminal_outcome(PgId::new(7), None, || {
                attempts += 1;
                Err(StoreError::PgNotFound { pg_id: 7 })
            })
            .unwrap_err();

        assert!(matches!(error, StoreError::PgNotFound { pg_id: 7 }));
        assert_eq!(attempts, 1);
    }
}
