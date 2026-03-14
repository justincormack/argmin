# XML Request Parser Migration

## Goal

Replace the hand-rolled request XML substring parsing in `crates/server-http/src/http/xml.rs`
with a real XML tokenizer while keeping:

- hand-written XML response generation
- explicit S3 semantic validation in local code
- current S3 error mapping (`MalformedXML`, `InvalidTag`, etc.)

This is a request-body parsing cleanup only. It is not a response rendering change.

## Why

The current parsers are simple, but they are now showing structural holes:

- malformed nesting can be accepted
- nested extraction depends on `find("<Tag>")` / `find("</Tag>")`
- each new request shape duplicates XML structure handling

The recent `POST Object` tagging bug was exactly this class of problem.

## Approach

Use `quick-xml` as a lightweight event reader.

- `quick-xml` handles XML well-formedness, nesting, and entity decoding
- local parser code handles S3 structure and validation rules
- no serde/data-binding layer

This keeps the validation logic explicit and reviewable.

## Status

Completed:

- `parse_tagging_xml(...)`
- `parse_delete_objects_xml(...)`
- `parse_versioning_config_xml(...)`
- `parse_cors_config_xml(...)`
- `parse_public_access_block_xml(...)`
- `parse_ownership_controls_xml(...)`
- `parse_complete_multipart_upload_xml(...)`

All request-body XML parsing in `server-http` now goes through `quick-xml`.

## Remaining Cleanup

The old substring helpers are no longer used by request parsers. They only remain for
non-request helpers, currently:

- `count_tags_in_xml(...)` test/support code

So the remaining work, if any, is just local cleanup:

- delete `extract_tag_content` / `extract_all_tag_contents` if the test helper is rewritten
- or leave them in place if the small test-only use is acceptable

## Non-goals

- replacing manual response XML generation
- introducing a generic XML object model
- changing S3 error semantics

## Notes

- Request XML bodies are small, so correctness matters more than parse throughput.
- This should stay in `server-http`; XML parsing is part of the transport boundary.
- Response XML generation remains deliberately hand-written.
