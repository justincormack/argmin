function canonicalize_function(name) {
    sub(/<.*/, "", name)
    if (name == "begin_bucket_delete_inner") {
        return "begin_bucket_delete"
    }
    if (name == "reclaim_object_payload_if_unleased_with_outcome") {
        return "reclaim_object_payload_if_unleased"
    }
    if (name == "reserve_next_object_version_with_completion_admission") {
        return "reserve_next_object_version"
    }
    if (name == "release_object_generation_reservation_with_work_budget") {
        return "release_object_generation_reservation"
    }
    if (name == "create_multipart_upload_inner") {
        return "create_multipart_upload"
    }
    if (name == "commit_stream_segment_append_with_work_budget") {
        return "commit_stream_segment_append"
    }
    if (name == "abort_stream_upload_session_with_work_budget") {
        return "abort_stream_upload_session"
    }
    return name
}

# Return only tokens that can affect item scope. Rust permits nested block
# comments and multi-line raw strings, so their lexer state is retained between
# lines while a test-only item is skipped.
function rust_code(line,    code, i, ch, next_ch, j, hashes, terminator, escaped) {
    code = ""
    i = 1
    while (i <= length(line)) {
        ch = substr(line, i, 1)
        next_ch = substr(line, i + 1, 1)

        if (lexer_block_comment_depth > 0) {
            if (ch == "/" && next_ch == "*") {
                lexer_block_comment_depth++
                i += 2
            } else if (ch == "*" && next_ch == "/") {
                lexer_block_comment_depth--
                i += 2
            } else {
                i++
            }
            continue
        }

        if (lexer_in_raw_string) {
            if (ch != "\"") {
                i++
                continue
            }
            terminator = "\""
            for (j = 0; j < lexer_raw_hashes; j++) {
                terminator = terminator "#"
            }
            if (substr(line, i, length(terminator)) == terminator) {
                lexer_in_raw_string = 0
                lexer_raw_hashes = 0
                i += length(terminator)
            } else {
                i++
            }
            continue
        }

        if (lexer_in_string) {
            if (ch == "\\") {
                i += 2
            } else if (ch == "\"") {
                lexer_in_string = 0
                i++
            } else {
                i++
            }
            continue
        }

        if (lexer_in_char) {
            if (ch == "\\") {
                i += 2
            } else if (ch == "'") {
                lexer_in_char = 0
                i++
            } else {
                i++
            }
            continue
        }

        if (ch == "/" && next_ch == "/") {
            break
        }
        if (ch == "/" && next_ch == "*") {
            lexer_block_comment_depth = 1
            i += 2
            continue
        }

        if (ch == "r") {
            j = i + 1
            hashes = 0
            while (substr(line, j, 1) == "#") {
                hashes++
                j++
            }
            if (substr(line, j, 1) == "\"") {
                lexer_in_raw_string = 1
                lexer_raw_hashes = hashes
                i = j + 1
                continue
            }
        }

        if (ch == "\"") {
            lexer_in_string = 1
            i++
            continue
        }

        if (ch == "'") {
            escaped = next_ch == "\\"
            if ((escaped && substr(line, i + 2) ~ /'/) \
                || (!escaped && substr(line, i + 2, 1) == "'")) {
                lexer_in_char = 1
                i++
                continue
            }
        }

        code = code ch
        i++
    }
    return code
}

function reset_rust_lexer() {
    lexer_block_comment_depth = 0
    lexer_in_raw_string = 0
    lexer_raw_hashes = 0
    lexer_in_string = 0
    lexer_in_char = 0
}

function scope_delta(line, copy, opens, closes) {
    copy = rust_code(line)
    scope_has_semicolon = copy ~ /;/
    sub(/[[:space:]]+$/, "", copy)
    scope_ends_with_comma = copy ~ /,$/
    opens = gsub(/\{/, "", copy)
    closes = gsub(/\}/, "", copy)
    scope_open_count = opens
    return opens - closes
}

function leading_indent(line, prefix) {
    prefix = line
    sub(/[^[:space:]].*$/, "", prefix)
    return length(prefix)
}

function is_sanctioned_test_cfg(line, compact) {
    compact = line
    gsub(/[[:space:]]/, "", compact)
    # Do not generalize this to every cfg expression containing `test`:
    # any(test, feature = "production-feature") is production-reachable.
    return compact == "#[cfg(test)]" \
        || compact == "#[cfg(any(test,feature=\"test-hooks\"))]" \
        || compact == "#[cfg(any(feature=\"test-hooks\",test))]"
}

function start_test_item(line, delta) {
    skipping_test_item = 1
    test_item_opened = 0
    test_item_balance = 0
    test_item_indent = leading_indent(line)
    reset_rust_lexer()
    delta = scope_delta(line)
    if (scope_open_count > 0) {
        test_item_opened = 1
    }
    test_item_balance += delta
    if ((test_item_opened && test_item_balance <= 0) \
        || (!test_item_opened && scope_has_semicolon) \
        || (!test_item_opened && scope_ends_with_comma \
            && leading_indent(line) <= test_item_indent)) {
        skipping_test_item = 0
    }
}

function consume_test_item(line, delta) {
    delta = scope_delta(line)
    if (scope_open_count > 0) {
        test_item_opened = 1
    }
    test_item_balance += delta
    if ((test_item_opened && test_item_balance <= 0) \
        || (!test_item_opened && scope_has_semicolon) \
        || (!test_item_opened && scope_ends_with_comma \
            && leading_indent(line) <= test_item_indent)) {
        skipping_test_item = 0
    }
}

function update_function_context(line) {
    if (line ~ /^    (pub(\([^)]*\))? |pub\(crate\) |pub\(super\) )?fn [A-Za-z0-9_]+/) {
        current_fn = line
        sub(/^.*fn /, "", current_fn)
        sub(/\(.*/, "", current_fn)
        current_fn = canonicalize_function(current_fn)
        current_publisher_id = ""
    }
}

function publisher_helper(line) {
    if (line ~ /MetadataCommandRecoveryDrainAuthority::new/) {
        return "metadata_command_recovery_authority_constructor"
    }
    if (line ~ /MetadataCommandDrainAuthority::for_recovery/) {
        return "metadata_command_recovery_invocation_constructor"
    }
    if (line ~ /\.admit_leader\(/) {
        return "metadata_command_recovery_leader_constructor"
    }
    if (line ~ /drain_pending_metadata_command_with_authority_inner/) {
        return "metadata_command_authorized_drain_primitive"
    }
    if (line ~ /drain_pending_metadata_command_with_recovery_authority/) {
        return "metadata_command_recovery_authority_drain"
    }
    if (line ~ /drain_pending_metadata_command_with_local_recovery_route/) {
        return "metadata_command_local_recovery_drain"
    }
    if (line ~ /drain_pending_metadata_command_with_authorized_recovery_(route|source)/) {
        return "metadata_command_historical_recovery_drain"
    }
    if (line ~ /MetadataCommandExecutionRoute::recovery/) {
        return "metadata_command_recovery_execution_route"
    }
    if (line ~ /MetadataCommandExecutionRoute[[:space:]]*\{/) {
        return "metadata_command_execution_route_struct_literal"
    }
    if (line ~ /\.for_reissued_command\(/) {
        return "metadata_command_recovery_proof_derivation"
    }
    if (line ~ /apply_reissued_metadata_command_to_acting_set_for_recovery/) {
        return "metadata_command_recovery_apply_reissued"
    }
    if (line ~ /apply_metadata_command_to_acting_set_for_recovery/) {
        return "metadata_command_recovery_apply"
    }
    if (line ~ /record_abandoned_metadata_command_to_acting_set_for_recovery/) {
        return "metadata_command_recovery_record_abandoned"
    }
    if (line ~ /remove_pending_metadata_command_for_bucket_recovery/) {
        return "metadata_command_recovery_remove_pending"
    }
    if (line ~ /reissue_pending_metadata_command_with_route_mode/) {
        return "metadata_command_recovery_reissue"
    }
    if (line ~ /finish_pending_metadata_command_to_acting_set_for_recovery_with_work_budget/) {
        return "metadata_command_recovery_bucket_finisher"
    }
    if (line ~ /finish_pending_metadata_command_recovery/) {
        return "retired_metadata_command_recovery_finisher"
    }
    if (line ~ /try_set_pending_metadata_command_for_bucket_with_effect_fence\(/) {
        return "try_set_pending_metadata_command_for_bucket_with_effect_fence"
    }
    if (line ~ /try_set_pending_metadata_command_for_bucket\(/) {
        return "try_set_pending_metadata_command_for_bucket"
    }
    if (line ~ /try_install_pending_metadata_command_for_bucket(_with_effect_fence)?\(/) {
        return "try_install_pending_metadata_command_for_bucket"
    }
    if (line ~ /try_install_object_pg_pending_command_with_fresh_id\(/) {
        return "try_install_object_pg_pending_command_with_fresh_id"
    }
    if (line ~ /try_set_object_pg_pending_command_or_drain\(/) {
        return "try_set_object_pg_pending_command_or_drain"
    }
    if (line ~ /try_install_object_pg_pending_command_or_drain\(/) {
        return "try_install_object_pg_pending_command_or_drain"
    }
    if (line ~ /try_set_bucket_pg_pending_command_or_retry(_with_work_budget(_and_effect_fence)?)?\(/) {
        return "try_set_bucket_pg_pending_command_or_retry"
    }
    if (line ~ /try_set_bucket_control_pending_command_or_retry(_with_work_budget)?\(/) {
        return "try_set_bucket_control_pending_command_or_retry"
    }
    if (line ~ /install_snapshot_sensitive_metadata_command_or_drain\(/) {
        return "install_snapshot_sensitive_metadata_command_or_drain"
    }
    if (line ~ /install_snapshot_sensitive_bucket_control_command_or_drain\(/) {
        return "install_snapshot_sensitive_bucket_control_command_or_drain"
    }
    if (line ~ /install_snapshot_sensitive_bucket_pg_command_or_drain\(/) {
        return "install_snapshot_sensitive_bucket_pg_command_or_drain"
    }
    if (line ~ /install_allocator_cleanup_metadata_command_with_fresh_id\(/) {
        return "install_allocator_cleanup_metadata_command_with_fresh_id"
    }
    if (line ~ /install_allocator_cleanup_pending_command_or_drain\(/) {
        return "install_allocator_cleanup_pending_command_or_drain"
    }
    if (line ~ /install_allocator_cleanup_bucket_pg_command_or_retry\(/) {
        return "install_allocator_cleanup_bucket_pg_command_or_retry"
    }
    if (line ~ /install_terminal_session_retry_metadata_command\(/) {
        return "install_terminal_session_retry_metadata_command"
    }
    if (line ~ /install_matching_outcome_retry_metadata_command\(/) {
        return "install_matching_outcome_retry_metadata_command"
    }
    if (line ~ /install_apply_validated_metadata_command_with_fresh_id\(/) {
        return "install_apply_validated_metadata_command_with_fresh_id"
    }
    if (line ~ /install_apply_validated_bucket_pg_command_or_retry\(/) {
        return "install_apply_validated_bucket_pg_command_or_retry"
    }
    if (line ~ /drain_one_pending_object_metadata_command\(/) {
        return "drain_one_pending_object_metadata_command"
    }
    if (line ~ /drain_pending_object_metadata_commands_for_publisher(_collect)?\(/) {
        return "drain_pending_object_metadata_commands_for_publisher"
    }
    if (line ~ /drain_pending_object_metadata_commands_for_bucket(_collect_unclassified)?\(/ \
        || line ~ /drain_pending_metadata_command_with_recovery_gate\(/ \
        || line ~ /drain_pending_metadata_command_with_recovery_gate_inner\(/) {
        return "raw_pending_object_metadata_command_drain"
    }
    if (line ~ /set_pending_metadata_command_for_bucket\(/) {
        return "set_pending_metadata_command_for_bucket"
    }
    return ""
}

function is_install_helper(name) {
    return name == "set_pending_metadata_command_for_bucket" \
        || name == "try_install_pending_metadata_command_for_bucket" \
        || name == "try_install_pending_metadata_command_for_bucket_with_effect_fence" \
        || name == "try_install_object_pg_pending_command_with_fresh_id" \
        || name == "try_set_object_pg_pending_command_or_drain" \
        || name == "try_install_object_pg_pending_command_or_drain" \
        || name == "try_set_bucket_pg_pending_command_or_retry" \
        || name == "try_set_bucket_pg_pending_command_or_retry_with_work_budget" \
        || name == "try_set_bucket_pg_pending_command_or_retry_with_work_budget_and_effect_fence" \
        || name == "try_set_bucket_control_pending_command_or_retry" \
        || name == "try_set_bucket_control_pending_command_or_retry_with_work_budget" \
        || name == "install_snapshot_sensitive_metadata_command_or_drain" \
        || name == "install_snapshot_sensitive_bucket_control_command_or_drain" \
        || name == "install_snapshot_sensitive_bucket_pg_command_or_drain" \
        || name == "install_allocator_cleanup_metadata_command_with_fresh_id" \
        || name == "install_allocator_cleanup_pending_command_or_drain" \
        || name == "install_allocator_cleanup_bucket_pg_command_or_retry" \
        || name == "install_terminal_session_retry_metadata_command" \
        || name == "install_matching_outcome_retry_metadata_command" \
        || name == "install_apply_validated_metadata_command_with_fresh_id" \
        || name == "install_apply_validated_bucket_pg_command_or_retry" \
        || name == "drain_one_pending_object_metadata_command" \
        || name == "drain_pending_object_metadata_commands_for_publisher" \
        || name == "drain_after_object_pg_log_conflict" \
        || name == "drain_pending_object_metadata_commands_for_publisher_collect" \
        || name == "next_object_metadata_command_id_or_drain_with_completion_admission" \
        || name == "raw_pending_object_metadata_command_drain" \
        || name == "try_set_pending_metadata_command_for_bucket" \
        || name == "try_set_pending_metadata_command_for_bucket_with_effect_fence"
}

FNR == 1 {
    current_fn = ""
    current_publisher_id = ""
    awaiting_publisher_id = 0
    pending_test_cfg = 0
    skipping_test_item = 0
}

skipping_test_item {
    consume_test_item($0)
    next
}

is_sanctioned_test_cfg($0) {
    pending_test_cfg = 1
    next
}

pending_test_cfg && $0 ~ /^[[:space:]]*#\[/ {
    next
}

pending_test_cfg && ($0 ~ /^[[:space:]]*$/ || $0 ~ /^[[:space:]]*\/\//) {
    next
}

pending_test_cfg {
    pending_test_cfg = 0
    start_test_item($0)
    next
}

{
    update_function_context($0)

    if ($0 ~ /metadata_command_publisher!\(/) {
        publisher_id = $0
        sub(/^.*metadata_command_publisher!\(/, "", publisher_id)
        sub(/\).*/, "", publisher_id)
        if (publisher_id != "") {
            current_publisher_id = publisher_id
            print "MARKER\t" publisher_id ":" current_fn
        } else {
            awaiting_publisher_id = 1
        }
        next
    }
    if (awaiting_publisher_id) {
        publisher_id = $0
        gsub(/[[:space:]]/, "", publisher_id)
        sub(/\).*/, "", publisher_id)
        current_publisher_id = publisher_id
        awaiting_publisher_id = 0
        print "MARKER\t" publisher_id ":" current_fn
        next
    }

    helper = publisher_helper($0)
    if (helper == "") {
        next
    }
    if (helper ~ /^metadata_command_.*recovery/ \
        || helper == "metadata_command_authorized_drain_primitive") {
        if ($0 ~ /^    (pub(\([^)]*\))? |pub\(crate\) |pub\(super\) )?fn /) {
            next
        }
        print "RECOVERY_DRAIN_USE\t" FILENAME ":" current_fn ":" helper
        next
    }
    if (is_install_helper(current_fn)) {
        next
    }
    # Phase 4 typed publisher APIs make the permitted helper a Rust trait
    # choice. Keep scanning inside the typed helper for raw bypasses, but do
    # not retain its already compiler-enforced callers in the shell inventory.
    if (helper == "install_snapshot_sensitive_metadata_command_or_drain" \
        || helper == "install_snapshot_sensitive_bucket_control_command_or_drain" \
        || helper == "install_snapshot_sensitive_bucket_pg_command_or_drain" \
        || helper == "install_allocator_cleanup_metadata_command_with_fresh_id" \
        || helper == "install_allocator_cleanup_pending_command_or_drain" \
        || helper == "install_allocator_cleanup_bucket_pg_command_or_retry" \
        || helper == "install_terminal_session_retry_metadata_command" \
        || helper == "install_matching_outcome_retry_metadata_command" \
        || helper == "install_apply_validated_metadata_command_with_fresh_id" \
        || helper == "install_apply_validated_bucket_pg_command_or_retry" \
        || helper == "drain_one_pending_object_metadata_command" \
        || helper == "drain_pending_object_metadata_commands_for_publisher") {
        if (current_publisher_id == "") {
            print "TYPED_MISSING_MARKER\t" FILENAME ":" current_fn ":" helper
        }
        next
    }
    publisher_id = current_publisher_id == "" ? "<missing>" : current_publisher_id
    print "PUBLISHER\t" FILENAME ":" current_fn ":" publisher_id ":" helper
}
