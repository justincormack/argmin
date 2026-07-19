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
}
