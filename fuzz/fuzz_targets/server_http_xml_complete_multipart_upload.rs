#![no_main]

use libfuzzer_sys::fuzz_target;
use server_http::http::serve::{fuzz_xml_parser_entrypoints, XmlFuzzInputs};

fuzz_target!(|data: &[u8]| {
    fuzz_xml_parser_entrypoints(XmlFuzzInputs {
        complete_multipart_upload_xml: data,
        delete_objects_xml: b"",
        bucket_lifecycle_xml: b"",
        bucket_cors_xml: b"",
        bucket_acl_xml: b"",
        bucket_object_lock_configuration_xml: b"",
        object_retention_xml: b"",
        object_legal_hold_xml: b"",
        bucket_versioning_xml: b"",
        bucket_encryption_xml: b"",
        bucket_tagging_xml: b"",
        object_tagging_xml: b"",
        public_access_block_xml: b"",
        ownership_controls_xml: b"",
    });
});
