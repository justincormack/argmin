function indentation(line) {
    match(line, /^[ ]*/)
    return RLENGTH
}

function opens_block(line) {
    return line ~ /\{[ ]*$/
}

function completes_signature_with_empty_body(line) {
    return line ~ /\{[ ]*}[ ]*$/
}

function has_semicolon(line) {
    return index(line, ";") != 0
}

function trim(line) {
    sub(/^[ ]+/, "", line)
    sub(/[ ]+$/, "", line)
    return line
}

function spaces(count, result) {
    result = ""
    while (length(result) < count) {
        result = result " "
    }
    return result
}

function raw_string_marker_length(line, offset, prefix_length, cursor, hashes, character) {
    prefix_length = 0
    character = substr(line, offset, 1)
    if ((character == "b" || character == "c") && substr(line, offset + 1, 1) == "r") {
        prefix_length = 2
    } else if (character == "r") {
        prefix_length = 1
    } else {
        return 0
    }

    cursor = offset + prefix_length
    hashes = 0
    while (substr(line, cursor, 1) == "#") {
        hashes++
        cursor++
    }
    if (substr(line, cursor, 1) != "\"") {
        return 0
    }

    detected_raw_hashes = hashes
    return cursor - offset + 1
}

function raw_string_closing_marker(hash_count, marker) {
    marker = "\""
    while (hash_count > 0) {
        marker = marker "#"
        hash_count--
    }
    return marker
}

function char_literal_length(line, offset, cursor, character, escaped) {
    if (substr(line, offset, 1) != "'") {
        return 0
    }

    cursor = offset + 1
    if (substr(line, cursor, 1) ~ /[A-Za-z_]/) {
        while (substr(line, cursor, 1) ~ /[A-Za-z0-9_]/) {
            cursor++
        }
        if (substr(line, cursor, 1) != "'") {
            return 0
        }
        return cursor - offset + 1
    }

    cursor = offset + 1
    escaped = 0
    while (cursor <= length(line)) {
        character = substr(line, cursor, 1)
        if (escaped) {
            escaped = 0
        } else if (character == "\\") {
            escaped = 1
        } else if (character == "'") {
            return cursor - offset + 1
        }
        cursor++
    }
    return 0
}

function strip_rust_noncode(line, output, length_of_line, offset, character, next_character, marker_length, closing_marker, literal_length) {
    output = ""
    length_of_line = length(line)
    offset = 1

    while (offset <= length_of_line) {
        character = substr(line, offset, 1)
        next_character = substr(line, offset + 1, 1)

        if (block_comment_depth > 0) {
            if (character == "/" && next_character == "*") {
                block_comment_depth++
                output = output "  "
                offset += 2
            } else if (character == "*" && next_character == "/") {
                block_comment_depth--
                output = output "  "
                offset += 2
            } else {
                output = output " "
                offset++
            }
            continue
        }

        if (raw_string_active) {
            closing_marker = raw_string_closing_marker(raw_string_hashes)
            if (substr(line, offset, length(closing_marker)) == closing_marker) {
                output = output spaces(length(closing_marker))
                offset += length(closing_marker)
                raw_string_active = 0
            } else {
                output = output " "
                offset++
            }
            continue
        }

        if (normal_string_active) {
            if (character == "\\") {
                output = output " "
                offset++
                if (offset <= length_of_line) {
                    output = output " "
                    offset++
                }
            } else {
                output = output " "
                offset++
                if (character == "\"") {
                    normal_string_active = 0
                }
            }
            continue
        }

        if (character == "/" && next_character == "/") {
            return output
        }
        if (character == "/" && next_character == "*") {
            block_comment_depth = 1
            output = output "  "
            offset += 2
            continue
        }

        marker_length = raw_string_marker_length(line, offset)
        if (marker_length > 0) {
            raw_string_active = 1
            raw_string_hashes = detected_raw_hashes
            output = output spaces(marker_length)
            offset += marker_length
            continue
        }

        if (character == "\"") {
            normal_string_active = 1
            output = output " "
            offset++
            continue
        }
        if ((character == "b" || character == "c") && next_character == "\"") {
            normal_string_active = 1
            output = output "  "
            offset += 2
            continue
        }

        literal_length = char_literal_length(line, offset)
        if (literal_length > 0) {
            output = output spaces(literal_length)
            offset += literal_length
            continue
        }
        if (character == "b" && next_character == "'") {
            literal_length = char_literal_length(line, offset + 1)
            if (literal_length > 0) {
                output = output spaces(literal_length + 1)
                offset += literal_length + 1
                continue
            }
        }

        output = output character
        offset++
    }

    return output
}

function declaration_intro_complete(text, count, values, i, token) {
    count = normalized_tokens(text, values)
    for (i = 1; i < count; i++) {
        token = values[i]
        if (token == "trait" || token == "struct" || token == "union" || token == "enum" ||
            token == "fn" || token == "type" || token == "use" ||
            token == "const" || token == "static" || token == "mod" ||
            token == "impl") {
            return 1
        }
    }
    return 0
}

function token_seen(text, wanted, count, values, i) {
    count = normalized_tokens(text, values)
    for (i = 1; i <= count; i++) {
        if (values[i] == wanted) {
            return 1
        }
    }
    return 0
}

function item_body_open_position(text, offset, character, previous, paren_depth, bracket_depth, angle_depth, nested_curly_depth) {
    paren_depth = 0
    bracket_depth = 0
    angle_depth = 0
    nested_curly_depth = 0

    for (offset = 1; offset <= length(text); offset++) {
        character = substr(text, offset, 1)
        previous = substr(text, offset - 1, 1)

        if (nested_curly_depth > 0) {
            if (character == "{") {
                nested_curly_depth++
            } else if (character == "}") {
                nested_curly_depth--
            }
            continue
        }

        if (character == "(") {
            paren_depth++
        } else if (character == ")" && paren_depth > 0) {
            paren_depth--
        } else if (character == "[") {
            bracket_depth++
        } else if (character == "]" && bracket_depth > 0) {
            bracket_depth--
        } else if (character == "<") {
            angle_depth++
        } else if (character == ">" && angle_depth > 0 && previous != "-") {
            angle_depth--
        } else if (character == "{") {
            if (paren_depth == 0 && bracket_depth == 0 && angle_depth == 0) {
                return offset
            }
            nested_curly_depth = 1
        }
    }
    return 0
}

function matching_closing_brace(text, opening, offset, depth, character) {
    depth = 0
    for (offset = opening; offset <= length(text); offset++) {
        character = substr(text, offset, 1)
        if (character == "{") {
            depth++
        } else if (character == "}") {
            depth--
            if (depth == 0) {
                return offset
            }
        }
    }
    return 0
}

function compact_impl_item_is_associated(item, normalized) {
    normalized = trim(item)
    while (normalized ~ /^#\[[^]]*\][ ]*/) {
        sub(/^#\[[^]]*\][ ]*/, "", normalized)
    }
    return normalized ~ /^(type|const)[ ]+/
}

function remember_compact_impl_item(item, start_line) {
    if (compact_impl_item_is_associated(item)) {
        remember_declaration(item, start_line)
    }
}

function scan_compact_impl_body(text, opening, closing, start_line, offset, character, item, paren_depth, bracket_depth, angle_depth, block_depth) {
    item = ""
    paren_depth = 0
    bracket_depth = 0
    angle_depth = 0
    block_depth = 0

    for (offset = opening + 1; offset < closing; offset++) {
        character = substr(text, offset, 1)

        if (block_depth > 0) {
            if (character == "{") {
                block_depth++
            } else if (character == "}") {
                block_depth--
            }
            continue
        }

        if (character == "(" ) {
            paren_depth++
        } else if (character == ")" && paren_depth > 0) {
            paren_depth--
        } else if (character == "[") {
            bracket_depth++
        } else if (character == "]" && bracket_depth > 0) {
            bracket_depth--
        } else if (character == "<") {
            angle_depth++
        } else if (character == ">" && angle_depth > 0) {
            angle_depth--
        }

        if (character == "{" && paren_depth == 0 && bracket_depth == 0 && angle_depth == 0) {
            remember_compact_impl_item(item, start_line)
            item = ""
            block_depth = 1
        } else if (character == ";" && paren_depth == 0 && bracket_depth == 0 && angle_depth == 0) {
            item = item character
            remember_compact_impl_item(item, start_line)
            item = ""
        } else {
            item = item character
        }
    }
}

function begin_split_declaration(line) {
    split_declaration_indent = indentation(line)
    split_declaration_start = FNR
    split_declaration_text = trim(line)
}

function append_split_declaration(line) {
    split_declaration_text = split_declaration_text " " trim(line)
}

function split_declaration_line(indent_prefix) {
    indent_prefix = sprintf("%" split_declaration_indent "s", "")
    return indent_prefix split_declaration_text
}

function normalized_tokens(text, values, normalized) {
    normalized = text
    gsub(/[^A-Za-z0-9_]/, " ", normalized)
    return split(normalized, values, /[ ]+/)
}

function remember_declaration(text, start_line) {
    declaration_count++
    declaration_text[declaration_count] = text
    declaration_file[declaration_count] = FILENAME
    declaration_line[declaration_count] = start_line
}

function remember_wildcard_import(start_line) {
    if (!scan_declarations) {
        return
    }
    wildcard_count++
    wildcard_file[wildcard_count] = FILENAME
    wildcard_line[wildcard_count] = start_line
}

function add_alias(name, target) {
    if (name != "" && name != "_") {
        alias_target[name] = alias_target[name] " " target
    }
}

function record_type_alias(text, equals_at, left, right, count, values, i, name) {
    equals_at = index(text, "=")
    if (equals_at == 0) {
        return
    }

    left = substr(text, 1, equals_at - 1)
    right = substr(text, equals_at + 1)
    count = normalized_tokens(left, values)
    for (i = 1; i < count; i++) {
        if (values[i] == "type") {
            name = values[i + 1]
            break
        }
    }
    add_alias(name, right)
}

function record_use_aliases(text, body, part_count, parts, i, count, values, j, alias, target) {
    body = text
    sub(/^[ ]*(pub([ ]*\([^)]*\))?[ ]+)?use[ ]+/, "", body)
    sub(/;[ ]*$/, "", body)

    # Public wildcard reexports can expose a physical type without leaving a
    # resolvable spelling in the public declaration. They are intentionally
    # forbidden in this small curated boundary rather than guessed through.
    # Private wildcard imports preserve the imported type's original name, so
    # a public signature using one is still checked literally.
    if (text ~ /^[ ]*pub[ ]+use[ ]+/ && body ~ /(^|[^A-Za-z0-9_])\*([^A-Za-z0-9_]|$)/) {
        remember_wildcard_import(alias_start)
    }

    # Rust import groups are comma-separated after brace removal. The source
    # prefix is irrelevant here: a renamed leaf is unsafe whenever its source
    # leaf resolves to a forbidden type.
    gsub(/[{}]/, "", body)
    part_count = split(body, parts, ",")
    for (i = 1; i <= part_count; i++) {
        count = normalized_tokens(parts[i], values)
        alias = ""
        target = ""
        for (j = 1; j < count; j++) {
            if (values[j] == "as") {
                alias = values[j + 1]
                break
            }
            target = target " " values[j]
        }
        if (alias != "") {
            add_alias(alias, target)
        }
    }
}

function finish_alias() {
    if (alias_kind == "type") {
        record_type_alias(alias_text)
    } else {
        record_use_aliases(alias_text)
    }
    alias_kind = ""
    alias_text = ""
}

function track_alias(line) {
    if (alias_kind != "") {
        alias_text = alias_text " " line
        if (has_semicolon(line)) {
            finish_alias()
        }
        return
    }

    if (line ~ /^[ ]*(pub([ ]*\([^)]*\))?[ ]+)?type[ ]+/) {
        alias_kind = "type"
    } else if (line ~ /^[ ]*(pub([ ]*\([^)]*\))?[ ]+)?use[ ]+/) {
        alias_kind = "use"
    } else {
        return
    }

    alias_start = logical_start
    alias_text = line
    if (has_semicolon(line)) {
        finish_alias()
    }
}

function text_contains_tainted_type(text, count, values, i, token) {
    count = normalized_tokens(text, values)
    for (i = 1; i <= count; i++) {
        token = values[i]
        if ((token in forbidden) || (token in tainted_alias)) {
            return 1
        }
    }
    return 0
}

function report_declaration(declaration_index, count, values, i, token, key) {
    count = normalized_tokens(declaration_text[declaration_index], values)
    for (i = 1; i <= count; i++) {
        token = values[i]
        if ((token in forbidden) || (token in tainted_alias)) {
            key = declaration_file[declaration_index] SUBSEP declaration_line[declaration_index] SUBSEP token
            if (!(key in reported)) {
                print declaration_file[declaration_index] ":" declaration_line[declaration_index] ":" token
                reported[key] = 1
            }
        }
    }
}

function begin_header(kind, line) {
    header_kind = kind
    header_indent = indentation(line)
    header_start = logical_start
    header_text = line
    if (header_kind == "impl") {
        maybe_finish_impl_header()
        return
    }
    if (opens_block(line) || completes_signature_with_empty_body(line) || has_semicolon(line)) {
        finish_header()
    }
}

function maybe_finish_impl_header(opening, closing, complete_text) {
    opening = item_body_open_position(header_text)
    if (opening == 0) {
        return
    }

    complete_text = header_text
    closing = matching_closing_brace(complete_text, opening)
    header_text = substr(complete_text, 1, opening)
    remember_declaration(header_text, header_start)
    if (closing == 0) {
        impl_indent = header_indent
    } else {
        scan_compact_impl_body(complete_text, opening, closing, header_start)
    }
    header_kind = ""
    header_text = ""
}

function finish_header() {
    remember_declaration(header_text, header_start)
    if (opens_block(header_text)) {
        if (header_kind == "trait") {
            trait_indent = header_indent
        } else if (header_kind == "struct" || header_kind == "union") {
            struct_indent = header_indent
        } else if (header_kind == "enum") {
            enum_indent = header_indent
        } else if (header_kind == "impl") {
            impl_indent = header_indent
        }
    }
    header_kind = ""
    header_text = ""
}

function begin_signature(kind, line) {
    signature_kind = kind
    signature_start = logical_start
    signature_text = line
    if (opens_block(line) || completes_signature_with_empty_body(line) || has_semicolon(line) || (kind == "field" && line ~ /,[ ]*$/)) {
        finish_signature()
    }
}

function finish_signature() {
    remember_declaration(signature_text, signature_start)
    signature_kind = ""
    signature_text = ""
}

BEGIN {
    forbidden["PgId"] = 1
    forbidden["BucketPgId"] = 1
    forbidden["ObjectMetadataPgId"] = 1
    forbidden["DataPgId"] = 1
    forbidden["NodeId"] = 1
    forbidden["ShardIndex"] = 1
    forbidden["ShardKey"] = 1
    forbidden["ShardLocation"] = 1
    forbidden["PgRouteSnapshot"] = 1
    forbidden["LocalPgRoute"] = 1
    forbidden["LocalClusterMap"] = 1
    forbidden["PgMetadataProof"] = 1
    forbidden["PgMetadataTransferProof"] = 1
    forbidden["GenerationId"] = 1
    forbidden["EcShape"] = 1
    forbidden["PayloadReclaimRoot"] = 1
    forbidden["BucketDeleteFinalizeRoot"] = 1
    forbidden["ObjectPayloadReclaimClaimRecord"] = 1
    forbidden["BucketDeleteFinalizeClaimRecord"] = 1
    forbidden["LifecycleSweepClaimRecord"] = 1
    forbidden["PlacedSegmentShardBackfillClaimRecord"] = 1
    forbidden["ShardScavengerObservation"] = 1
    forbidden["ObjectSegmentRecord"] = 1
    forbidden["MultipartPartRecord"] = 1
    forbidden["StreamUploadSegmentRecord"] = 1
    forbidden["PgMetadataStore"] = 1
    forbidden["SharedStorageNode"] = 1
}

FNR == 1 {
    inline_api_active = 0
    scan_declarations = index(FILENAME, api_module_root "/") == 1
    trait_indent = -1
    struct_indent = -1
    enum_indent = -1
    impl_indent = -1
    header_kind = ""
    signature_kind = ""
    alias_kind = ""
    split_declaration_text = ""
    block_comment_depth = 0
    raw_string_active = 0
    normal_string_active = 0
}

{
    line = strip_rust_noncode($0)
    logical_start = FNR

    if (split_declaration_text != "") {
        if (line ~ /^[ ]*$/ || line ~ /^[ ]*\/\//) {
            next
        }
        append_split_declaration(line)
        if (!declaration_intro_complete(split_declaration_text) &&
            !(struct_indent >= 0 && split_declaration_text ~ /:/)) {
            next
        }
        line = split_declaration_line()
        logical_start = split_declaration_start
        split_declaration_text = ""
    } else if ((line ~ /^[ ]*pub([ ]*\([^)]*\))?([ ]+.*)?$/ &&
                !declaration_intro_complete(line) &&
                !(struct_indent >= 0 && line ~ /:/)) ||
               (trait_indent >= 0 && indentation(line) == trait_indent + 4 &&
                line ~ /^[ ]*(async|const|unsafe|extern([ ]+[^ ]+)?|fn)[ ]*$/) ||
               (impl_indent >= 0 && indentation(line) == impl_indent + 4 &&
                line ~ /^[ ]*(const|type)[ ]*$/) ||
               line ~ /^[ ]*(unsafe[ ]+)?impl[ ]*$/ ||
               line ~ /^[ ]*(type|use)[ ]*$/) {
        begin_split_declaration(line)
        next
    }

    if (FILENAME == inline_root && !inline_api_active) {
        track_alias(line)
        if (line ~ /^pub mod test_support[ ]*\{[ ]*$/) {
            inline_api_active = 1
            scan_declarations = 1
        }
        next
    }
    if (FILENAME == inline_root && line ~ /^}[ ]*$/) {
        inline_api_active = 0
        scan_declarations = 0
        next
    }

    track_alias(line)
    if (!scan_declarations) {
        next
    }

    if (header_kind != "") {
        header_text = header_text " " line
        if (header_kind == "impl") {
            maybe_finish_impl_header()
        } else if (opens_block(line) || completes_signature_with_empty_body(line) || has_semicolon(line)) {
            finish_header()
        }
        next
    }

    if (signature_kind != "") {
        signature_text = signature_text " " line
        if (opens_block(line) || completes_signature_with_empty_body(line) || has_semicolon(line) || (signature_kind == "field" && line ~ /,[ ]*$/)) {
            finish_signature()
        }
        next
    }

    indent = indentation(line)

    if (trait_indent >= 0 && indent == trait_indent && line ~ /^[ ]*}[ ]*$/) {
        trait_indent = -1
        next
    }
    if (struct_indent >= 0 && indent == struct_indent && line ~ /^[ ]*}[ ]*;?[ ]*$/) {
        struct_indent = -1
        next
    }
    if (enum_indent >= 0 && indent == enum_indent && line ~ /^[ ]*}[ ]*$/) {
        enum_indent = -1
        next
    }
    if (impl_indent >= 0 && indent == impl_indent && line ~ /^[ ]*}[ ]*$/) {
        impl_indent = -1
        next
    }

    if (trait_indent >= 0) {
        if (indent == trait_indent + 4 && line ~ /^[ ]*((async|const|unsafe)[ ]+)*(extern([ ]+"[^"]*")?[ ]+)?fn[ ]+/) {
            begin_signature("trait_method", line)
        } else if (indent == trait_indent + 4 && line ~ /^[ ]*(type|const)[ ]+/) {
            begin_signature("trait_item", line)
        }
        next
    }

    if (struct_indent >= 0) {
        if (indent == struct_indent + 4 && line ~ /^[ ]*pub([ ]*\([^)]*\))?[ ]+/) {
            begin_signature("field", line)
        }
        next
    }

    if (enum_indent >= 0) {
        if (line !~ /^[ ]*(#|\/\/)/) {
            remember_declaration(line, logical_start)
        }
        next
    }

    if (impl_indent >= 0) {
        if (indent == impl_indent + 4 && line ~ /^[ ]*(type|const)[ ]+/) {
            begin_signature("impl_item", line)
        } else if (indent == impl_indent + 4 &&
                   line ~ /^[ ]*pub[ ]+((async|const|unsafe)[ ]+)*(extern([ ]+"[^"]*")?[ ]+)?fn[ ]+/) {
            begin_signature("function", line)
        } else if (indent == impl_indent + 4 && line ~ /^[ ]*pub[ ]+(type|const|static)[ ]+/) {
            begin_signature("item", line)
        }
        next
    }

    if (line ~ /^[ ]*pub[ ]+trait[ ]+/) {
        begin_header("trait", line)
    } else if (line ~ /^[ ]*pub[ ]+struct[ ]+/) {
        begin_header("struct", line)
    } else if (line ~ /^[ ]*pub[ ]+union[ ]+/) {
        begin_header("union", line)
    } else if (line ~ /^[ ]*pub[ ]+enum[ ]+/) {
        begin_header("enum", line)
    } else if (line ~ /^[ ]*(unsafe[ ]+)?impl([ ]|<)/) {
        begin_header("impl", line)
    } else if (line ~ /^[ ]*pub[ ]+((async|const|unsafe)[ ]+)*(extern([ ]+"[^"]*")?[ ]+)?fn[ ]+/) {
        begin_signature("function", line)
    } else if (line ~ /^[ ]*pub[ ]+(type|use|const|static)[ ]+/) {
        begin_signature("item", line)
    }
}

END {
    # Resolve alias chains to a fixed point. Overlapping aliases in distinct
    # test-support modules are deliberately conservative: any public use of a
    # name which is physical in one curated module is rejected for review.
    do {
        changed = 0
        for (name in alias_target) {
            if (!(name in tainted_alias) && text_contains_tainted_type(alias_target[name])) {
                tainted_alias[name] = 1
                changed = 1
            }
        }
    } while (changed)

    for (i = 1; i <= declaration_count; i++) {
        report_declaration(i)
    }
    for (i = 1; i <= wildcard_count; i++) {
        print wildcard_file[i] ":" wildcard_line[i] ":wildcard-import"
    }
}
