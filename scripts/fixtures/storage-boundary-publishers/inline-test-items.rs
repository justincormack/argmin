enum FixtureMode {
    #[cfg(test)]
    TestOnlyVariant,
    ProductionVariant,
}

struct Fixture {
    #[cfg(test)]
    test_only_field: (),
}

#[cfg(test)]
mod differently_named_inline_checks {
    fn test_only_publisher_cannot_satisfy_registry() {
        let unmatched_string_brace = "{";
        let unmatched_raw_string_brace = r#"{"#;
        let unmatched_character_brace = '{';
        // {
        /*
         * {
         */
        crate::metadata_command::metadata_command_publisher!(TestOnlyModulePublisher);
        try_set_pending_metadata_command_for_bucket();
    }
}

impl Fixture {
    fn tail_expression_constructor() -> Self {
        Self {
            #[cfg(test)]
            test_only_field: (),
        }
    }

    fn production_publisher_after_inline_test_module() {
        crate::metadata_command::metadata_command_publisher!(FixtureProductionPublisher);
        try_set_pending_metadata_command_for_bucket();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_only_item_cannot_contaminate_inventory() {
        crate::metadata_command::metadata_command_publisher!(TestOnlyItemPublisher);
        try_install_pending_metadata_command_for_bucket();
    }

    #[cfg(any(test, feature = "live-feature"))]
    fn production_feature_publisher_must_remain_visible() {
        crate::metadata_command::metadata_command_publisher!(LiveFeaturePublisher);
        try_install_pending_metadata_command_for_bucket();
    }

    fn typed_snapshot_sensitive_publisher_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedSnapshotPublisher);
        install_snapshot_sensitive_metadata_command_or_drain();
    }

    fn typed_snapshot_sensitive_publisher_without_marker_is_rejected() {
        install_snapshot_sensitive_metadata_command_or_drain();
    }

    fn typed_snapshot_sensitive_bucket_control_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedSnapshotBucketControlPublisher);
        install_snapshot_sensitive_bucket_control_command_or_drain();
    }

    fn typed_snapshot_sensitive_bucket_control_without_marker_is_rejected() {
        install_snapshot_sensitive_bucket_control_command_or_drain();
    }

    fn typed_snapshot_sensitive_bucket_pg_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedSnapshotBucketPgPublisher);
        install_snapshot_sensitive_bucket_pg_command_or_drain();
    }

    fn typed_snapshot_sensitive_bucket_pg_without_marker_is_rejected() {
        install_snapshot_sensitive_bucket_pg_command_or_drain();
    }

    fn typed_allocator_cleanup_publisher_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedAllocatorPublisher);
        install_allocator_cleanup_metadata_command_with_fresh_id();
    }

    fn typed_allocator_cleanup_publisher_without_marker_is_rejected() {
        install_allocator_cleanup_metadata_command_with_fresh_id();
    }

    fn typed_terminal_session_publisher_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedTerminalSessionPublisher);
        install_terminal_session_retry_metadata_command();
    }

    fn typed_terminal_session_publisher_without_marker_is_rejected() {
        install_terminal_session_retry_metadata_command();
    }

    fn typed_matching_outcome_publisher_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedMatchingOutcomePublisher);
        install_matching_outcome_retry_metadata_command();
    }

    fn typed_matching_outcome_publisher_without_marker_is_rejected() {
        install_matching_outcome_retry_metadata_command();
    }

    fn typed_apply_validated_publisher_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedApplyValidatedPublisher);
        install_apply_validated_metadata_command_with_fresh_id();
    }

    fn typed_apply_validated_publisher_without_marker_is_rejected() {
        install_apply_validated_metadata_command_with_fresh_id();
    }

    fn typed_generic_drain_is_not_shell_inventoried() {
        crate::metadata_command::metadata_command_publisher!(TypedGenericDrainPublisher);
        drain_one_pending_object_metadata_command();
    }

    fn typed_generic_drain_without_marker_is_rejected() {
        drain_pending_object_metadata_commands_for_publisher();
    }

    fn release_object_generation_reservation_with_work_budget() {
        crate::metadata_command::metadata_command_publisher!(CanonicalizedReleasePublisher);
        install_allocator_cleanup_pending_command_or_drain();
    }

    fn raw_generic_drain_from_publisher_is_rejected() {
        crate::metadata_command::metadata_command_publisher!(RawGenericDrainPublisher);
        drain_pending_object_metadata_commands_for_bucket();
    }

    fn raw_generic_drain_from_unmarked_wrapper_is_rejected() {
        drain_pending_object_metadata_commands_for_bucket();
    }

    fn raw_inner_drain_from_unmarked_wrapper_is_rejected() {
        drain_pending_metadata_command_with_authority_inner();
    }

    fn recovery_authority_construction_from_unmarked_wrapper_is_rejected() {
        MetadataCommandRecoveryDrainAuthority::new();
    }

    fn recovery_authority_constructor_function_item_is_rejected() {
        let _mint = MetadataCommandRecoveryDrainAuthority::new;
    }

    fn recovery_invocation_construction_from_unmarked_wrapper_is_rejected() {
        MetadataCommandDrainAuthority::for_recovery();
    }

    fn recovery_wrapper_from_unmarked_wrapper_is_rejected() {
        drain_pending_metadata_command_with_local_recovery_route();
    }

    fn recovery_execution_route_from_unmarked_wrapper_is_rejected() {
        MetadataCommandExecutionRoute::recovery();
    }

    fn recovery_execution_route_struct_literal_is_rejected() {
        let _route = MetadataCommandExecutionRoute { mode: MetadataCommandRouteMode::Recovery };
    }

    fn recovery_proof_derivation_from_unmarked_wrapper_is_rejected() {
        route.for_reissued_command();
    }

    fn recovery_leader_construction_from_unmarked_wrapper_is_rejected() {
        authority.admit_leader();
    }

    fn recovery_apply_from_unmarked_wrapper_is_rejected() {
        apply_metadata_command_to_acting_set_for_recovery();
    }

    fn recovery_reissued_apply_from_unmarked_wrapper_is_rejected() {
        apply_reissued_metadata_command_to_acting_set_for_recovery();
    }

    fn recovery_abandon_from_unmarked_wrapper_is_rejected() {
        record_abandoned_metadata_command_to_acting_set_for_recovery();
    }

    fn recovery_pending_removal_from_unmarked_wrapper_is_rejected() {
        remove_pending_metadata_command_for_bucket_recovery();
    }

    fn recovery_reissue_from_unmarked_wrapper_is_rejected() {
        reissue_pending_metadata_command_with_route_mode();
    }

    fn recovery_bucket_finisher_from_unmarked_wrapper_is_rejected() {
        finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget();
    }

    fn retired_recovery_finisher_from_unmarked_wrapper_is_rejected() {
        finish_pending_metadata_command_recovery_with_work_budget();
    }

    fn publisher_token_constructor_function_item_is_rejected() {
        let mint =
            crate::metadata_command::publisher::TypedSnapshotPublisher::__from_registry_marker;
        let _token = mint();
    }
}
