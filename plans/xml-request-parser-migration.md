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

## Scope

### Phase 1

Port `parse_tagging_xml(...)` to `quick-xml`.

Success criteria:

- all existing tagging unit tests still pass
- the malformed POST tagging regression still passes
- error mapping stays the same:
  - malformed structure -> `MalformedXML`
  - semantically invalid tags -> `InvalidTag`

### Phase 2

Port the other request XML parsers one by one:

- `parse_delete_objects_xml`
- `parse_versioning_config_xml`
- CORS config parser
- ownership controls parser
- public access block parser
- multipart complete XML parser

Each parser should move in its own small cut with targeted regression coverage.

### Phase 3

Delete or narrow the generic substring helpers once no request parser depends on them.

Candidates:

- `extract_tag_content`
- `extract_all_tag_contents`
- ad hoc nested content scanning that only exists for request parsing

## Non-goals

- replacing manual response XML generation
- introducing a generic XML object model
- changing S3 error semantics

## Notes

- Request XML bodies are small, so correctness matters more than parse throughput.
- This should stay in `server-http`; XML parsing is part of the transport boundary.
