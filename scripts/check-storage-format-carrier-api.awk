function brace_delta(line, opens, closes, copy) {
    copy = line
    opens = gsub(/\{/, "{", copy)
    copy = line
    closes = gsub(/\}/, "}", copy)
    return opens - closes
}

function carrier_name(header) {
    if (header ~ /StoredTagSet/) {
        return "StoredTagSet"
    }
    if (header ~ /StoredAclGrants/) {
        return "StoredAclGrants"
    }
    return ""
}

function method_is_allowed(carrier, method) {
    if (carrier == "StoredTagSet") {
        return method == "from_tag_set" || method == "parse_current" \
            || method == "tag_set" || method == "as_storage_str"
    }
    return method == "from_grants" || method == "parse_current" \
        || method == "grants" || method == "into_grants" \
        || method == "as_storage_str"
}

FNR == 1 {
    header = ""
    in_carrier_impl = 0
    depth = 0
    carrier = ""
}

!in_carrier_impl {
    if (header == "" && $0 ~ /^[[:space:]]*impl([[:space:]<]|$)/) {
        header = $0
    } else if (header != "") {
        header = header " " $0
    }

    if (header != "" && header ~ /\{/) {
        candidate = carrier_name(header)
        if (candidate != "" && header ~ /impl[[:space:]]+(From|TryFrom|Into|TryInto)[^\{]*</) {
            print FILENAME ":" FNR ":conversion-trait:" candidate
        }
        if (candidate != "" && header ~ /impl([[:space:]<][^\{]*)?[[:space:]]Stored(TagSet|AclGrants)([[:space:]]+where[^\{]*)?[[:space:]]*\{/) {
            in_carrier_impl = 1
            carrier = candidate
            depth = brace_delta(header)
        }
        header = ""
    }
    next
}

in_carrier_impl {
    if (match($0, /^[[:space:]]*pub([[:space:]]*\([^)]*\))?[[:space:]]+(const[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]]+([A-Za-z0-9_]+)/, parts)) {
        method = parts[4]
        if (!method_is_allowed(carrier, method)) {
            print FILENAME ":" FNR ":public-method:" carrier "::" method
        }
    }
    depth += brace_delta($0)
    if (depth <= 0) {
        in_carrier_impl = 0
        carrier = ""
    }
}
