// Differential AWS-vs-local integration test package.

pub fn require_external_diff_test_env() {
    for (name, detail) in [
        (
            "S3_TEST_ENDPOINT",
            "the AWS-compatible endpoint to compare against",
        ),
        (
            "S3_TEST_ACCESS_KEY",
            "the primary AWS access key for differential comparisons",
        ),
        (
            "S3_TEST_SECRET_KEY",
            "the primary AWS secret key for differential comparisons",
        ),
        (
            "S3_TEST_ACCOUNT_ID",
            "the primary AWS account ID for differential comparisons",
        ),
        (
            "S3_TEST_ALT_ACCESS_KEY",
            "the alternate-account AWS access key for differential comparisons",
        ),
        (
            "S3_TEST_ALT_SECRET_KEY",
            "the alternate-account AWS secret key for differential comparisons",
        ),
        (
            "S3_TEST_ALT_ACCOUNT_ID",
            "the alternate AWS account ID for differential comparisons",
        ),
        (
            "S3_TEST_BUCKET_PREFIX",
            "the dedicated external bucket prefix for differential comparisons",
        ),
    ] {
        std::env::var(name).unwrap_or_else(|_| {
            panic!(
                "s3-diff-tests require {name} ({detail}); provide the full external S3_TEST_* AWS comparison configuration and run the differential suite with `cargo test --manifest-path crates/s3-diff-tests/Cargo.toml ...`"
            )
        });
    }
}

#[cfg(test)]
mod tests {
    use super::require_external_diff_test_env;

    #[test]
    fn diff_tests_require_external_config() {
        require_external_diff_test_env();
    }
}
