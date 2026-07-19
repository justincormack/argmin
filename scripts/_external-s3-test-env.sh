#!/usr/bin/env bash

external_s3_test_die() {
    echo "error: $*" >&2
    exit 2
}

external_s3_test_repo_root() {
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd
}

external_s3_test_load_dotenv() {
    local dotenv_path="$1"
    local assignments

    [[ -f "$dotenv_path" ]] || external_s3_test_die "missing env file: $dotenv_path"

    assignments="$(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$dotenv_path" || true)"
    [[ -n "$assignments" ]] || external_s3_test_die "no KEY=VALUE entries found in $dotenv_path"

    eval "$assignments"
}

external_s3_test_require_shell_var() {
    local name="$1"
    [[ -n "${!name:-}" ]] || external_s3_test_die "missing $name in .env"
}

external_s3_test_default_endpoint() {
    local region="$1"
    printf 'https://s3.%s.amazonaws.com\n' "$region"
}

external_s3_test_default_s3_control_endpoint() {
    local account_id="$1"
    local region="$2"
    printf 'https://%s.s3-control.%s.amazonaws.com\n' "$account_id" "$region"
}

external_s3_test_export_common_env() {
    local region="$1"
    local endpoint="$2"
    local bucket_prefix="$3"
    local timeout_secs="$4"
    local s3_control_endpoint="$5"

    external_s3_test_require_shell_var TEST_AWS_PRIMARY_ACCESS_KEY
    external_s3_test_require_shell_var TEST_AWS_PRIMARY_SECRET_KEY
    external_s3_test_require_shell_var TEST_AWS_PRIMARY_ACCOUNT_ID
    external_s3_test_require_shell_var TEST_AWS_ALT_ACCESS_KEY
    external_s3_test_require_shell_var TEST_AWS_ALT_SECRET_KEY
    external_s3_test_require_shell_var TEST_AWS_ALT_ACCOUNT_ID

    export S3_TEST_ENDPOINT="$endpoint"
    export S3_CONTROL_TEST_ENDPOINT="$s3_control_endpoint"
    export AWS_TEST_ACCESS_KEY="$TEST_AWS_PRIMARY_ACCESS_KEY"
    export AWS_TEST_SECRET_KEY="$TEST_AWS_PRIMARY_SECRET_KEY"
    export AWS_TEST_ACCOUNT_ID="$TEST_AWS_PRIMARY_ACCOUNT_ID"
    export AWS_TEST_ALT_ACCESS_KEY="$TEST_AWS_ALT_ACCESS_KEY"
    export AWS_TEST_ALT_SECRET_KEY="$TEST_AWS_ALT_SECRET_KEY"
    export AWS_TEST_ALT_ACCOUNT_ID="$TEST_AWS_ALT_ACCOUNT_ID"
    export AWS_TEST_REGION="$region"
    export S3_TEST_BUCKET_PREFIX="$bucket_prefix"
    export S3_TEST_TIMEOUT_SECS="$timeout_secs"

    if [[ -n "${TEST_AWS_SECOND_ACCESS_KEY:-}" || -n "${TEST_AWS_SECOND_SECRET_KEY:-}" ]]; then
        external_s3_test_require_shell_var TEST_AWS_SECOND_ACCESS_KEY
        external_s3_test_require_shell_var TEST_AWS_SECOND_SECRET_KEY
        export AWS_TEST_SECOND_ACCESS_KEY="$TEST_AWS_SECOND_ACCESS_KEY"
        export AWS_TEST_SECOND_SECRET_KEY="$TEST_AWS_SECOND_SECRET_KEY"
    fi

    if [[ -n "${TEST_AWS_OWNER_ROOT_ACCESS_KEY:-}" || -n "${TEST_AWS_OWNER_ROOT_SECRET_KEY:-}" ]]; then
        external_s3_test_require_shell_var TEST_AWS_OWNER_ROOT_ACCESS_KEY
        external_s3_test_require_shell_var TEST_AWS_OWNER_ROOT_SECRET_KEY
        export AWS_TEST_OWNER_ROOT_ACCESS_KEY="$TEST_AWS_OWNER_ROOT_ACCESS_KEY"
        export AWS_TEST_OWNER_ROOT_SECRET_KEY="$TEST_AWS_OWNER_ROOT_SECRET_KEY"
    fi
}

external_s3_test_print_command() {
    local label="$1"
    shift

    echo "== $label =="
    printf '+'
    printf ' %q' "$@"
    printf '\n'
}
