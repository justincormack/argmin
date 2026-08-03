BEGIN {
    FS = "\t"
    failed = 0
}

function fail(message) {
    print "error: " message > "/dev/stderr"
    failed = 1
}

$1 == "package" {
    name = $2
    if (name == "") {
        fail("package inventory contains an empty package name")
        next
    }
    if (name in packages) {
        fail("package inventory contains duplicate package " name)
        next
    }
    packages[name] = 1
    publish_disabled[name] = $3
    next
}

$1 == "allow" {
    name = $2
    if (name == "") {
        fail("test-only allowlist contains an empty package name")
        next
    }
    if (name in allowed) {
        fail("test-only allowlist contains duplicate package " name)
        next
    }
    allowed[name] = 1
    next
}

$1 == "enabled" {
    name = $2
    feature = $3
    if (name == "" || feature == "") {
        fail("enabled-feature inventory contains an incomplete record")
        next
    }
    enabled[name] = 1
    if (enabled_features[name] == "") {
        enabled_features[name] = feature
    } else {
        enabled_features[name] = enabled_features[name] ", " feature
    }
    next
}

NF != 0 {
    fail("unknown inventory record kind " $1)
}

END {
    for (name in allowed) {
        if (!(name in packages)) {
            fail("test-only allowlist contains non-workspace package " name)
        } else if (publish_disabled[name] != "true") {
            fail("hook-enabled test-only package must set publish = false: " name)
        }
        if (!(name in enabled)) {
            fail("test-only allowlist entry no longer enables test support: " name)
        }
    }

    for (name in enabled) {
        if (!(name in packages)) {
            fail("enabled-feature inventory references non-workspace package " name)
        } else if (!(name in allowed)) {
            fail("workspace package enables test support through its normal/build graph: " \
                name " (" enabled_features[name] ")")
        }
    }

    exit failed
}
