BEGIN IMMEDIATE;

CREATE TABLE staging_intents (
    pg_id                       INTEGER NOT NULL CHECK (pg_id >= 0),
    transition_epoch            INTEGER NOT NULL CHECK (transition_epoch > 0),
    source_epoch                INTEGER NOT NULL CHECK (source_epoch > 0),
    source_acting_set           BLOB NOT NULL,
    destination_acting_set      BLOB NOT NULL,
    staging_generation          INTEGER NOT NULL CHECK (staging_generation > 0),
    artifact_digest             BLOB NOT NULL CHECK (length(artifact_digest) = 32),
    artifact_length             INTEGER NOT NULL CHECK (artifact_length >= 0),
    artifact_format_version     INTEGER NOT NULL CHECK (artifact_format_version = 3),
    state                       INTEGER NOT NULL CHECK (state IN (0, 1, 2, 3)),
    publication_receipt         BLOB,
    CHECK (transition_epoch = staging_generation),
    CHECK (
        (state = 0 AND publication_receipt IS NULL)
        OR (state IN (1, 2) AND publication_receipt IS NOT NULL AND length(publication_receipt) BETWEEN 1 AND 4096)
        OR (state = 3 AND (publication_receipt IS NULL OR length(publication_receipt) BETWEEN 1 AND 4096))
    ),
    PRIMARY KEY (pg_id, staging_generation)
) STRICT;

CREATE TABLE staging_finalized_floors (
    pg_id                       INTEGER PRIMARY KEY CHECK (pg_id >= 0),
    staging_generation          INTEGER NOT NULL CHECK (staging_generation > 0)
) STRICT;

CREATE TABLE staging_evidence_actor (
    singleton                    INTEGER PRIMARY KEY CHECK (singleton = 1),
    node_id                      INTEGER NOT NULL CHECK (node_id >= 0 AND node_id <= 4294967295),
    node_incarnation             INTEGER NOT NULL CHECK (node_incarnation > 0),
    endpoint                     TEXT NOT NULL CHECK (length(endpoint) BETWEEN 1 AND 2048)
) STRICT;

CREATE TABLE staging_evidence_actor_closure_candidate (
    singleton                    INTEGER PRIMARY KEY CHECK (singleton = 1),
    first_node_id                INTEGER NOT NULL CHECK (first_node_id >= 0 AND first_node_id <= 4294967295),
    first_node_incarnation       INTEGER NOT NULL CHECK (first_node_incarnation > 0),
    first_endpoint               TEXT NOT NULL CHECK (length(first_endpoint) BETWEEN 1 AND 2048),
    first_accepted_generation    INTEGER NOT NULL CHECK (first_accepted_generation >= 0),
    first_accepted_apply_receipt_digest BLOB NOT NULL CHECK (length(first_accepted_apply_receipt_digest) = 32),
    first_ambiguous_generation   INTEGER CHECK (first_ambiguous_generation IS NULL OR first_ambiguous_generation > 0),
    first_ambiguous_page_digest  BLOB CHECK (first_ambiguous_page_digest IS NULL OR length(first_ambiguous_page_digest) = 32),
    first_ambiguous_apply_receipt_digest BLOB CHECK (first_ambiguous_apply_receipt_digest IS NULL OR length(first_ambiguous_apply_receipt_digest) = 32),
    through_node_id              INTEGER NOT NULL CHECK (through_node_id >= 0 AND through_node_id <= 4294967295),
    through_node_incarnation     INTEGER NOT NULL CHECK (through_node_incarnation > 0),
    through_endpoint             TEXT NOT NULL CHECK (length(through_endpoint) BETWEEN 1 AND 2048),
    rebound_entry_count          INTEGER NOT NULL CHECK (rebound_entry_count > 0),
    rebound_max_sequence         INTEGER NOT NULL CHECK (rebound_max_sequence > 0),
    rebound_evidence_digest      BLOB NOT NULL CHECK (length(rebound_evidence_digest) = 32),
    CHECK (first_node_id = through_node_id),
    CHECK (first_node_incarnation <= through_node_incarnation),
    CHECK (rebound_entry_count <= rebound_max_sequence),
    CHECK (
        (first_accepted_generation = 0 AND hex(first_accepted_apply_receipt_digest) = '0000000000000000000000000000000000000000000000000000000000000000')
        OR (first_accepted_generation > 0 AND hex(first_accepted_apply_receipt_digest) != '0000000000000000000000000000000000000000000000000000000000000000')
    ),
    CHECK (
        (first_ambiguous_generation IS NULL AND first_ambiguous_page_digest IS NULL AND first_ambiguous_apply_receipt_digest IS NULL)
        OR (first_ambiguous_generation = first_accepted_generation + 1 AND first_ambiguous_page_digest IS NOT NULL AND first_ambiguous_apply_receipt_digest IS NOT NULL)
    ),
    CHECK (first_accepted_generation > 0 OR first_ambiguous_generation IS NOT NULL)
) STRICT;

CREATE TABLE staging_evidence_deltas (
    sequence                    INTEGER PRIMARY KEY AUTOINCREMENT,
    pg_id                       INTEGER NOT NULL CHECK (pg_id >= 0),
    staging_generation          INTEGER NOT NULL CHECK (staging_generation > 0),
    evidence_kind               INTEGER NOT NULL CHECK (evidence_kind IN (0, 1)),
    target_epoch                INTEGER NOT NULL CHECK (
        (evidence_kind = 0 AND target_epoch > 0)
        OR (evidence_kind = 1 AND target_epoch = 0)
    ),
    actor_node_id               INTEGER NOT NULL CHECK (actor_node_id >= 0 AND actor_node_id <= 4294967295),
    actor_node_incarnation      INTEGER NOT NULL CHECK (actor_node_incarnation > 0),
    actor_endpoint              TEXT NOT NULL CHECK (length(actor_endpoint) BETWEEN 1 AND 2048),
    evidence_bytes              BLOB NOT NULL CHECK (length(evidence_bytes) BETWEEN 1 AND 4096),
    acknowledged                INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
    UNIQUE (pg_id, staging_generation, evidence_kind, target_epoch)
) STRICT;

CREATE TRIGGER staging_evidence_actor_insert
BEFORE INSERT ON staging_evidence_deltas
WHEN NOT EXISTS (
    SELECT 1 FROM staging_evidence_actor
    WHERE singleton = 1
      AND node_id = NEW.actor_node_id
      AND node_incarnation = NEW.actor_node_incarnation
      AND endpoint = NEW.actor_endpoint
)
BEGIN
    SELECT RAISE(ABORT, 'staging evidence actor is stale');
END;

CREATE TABLE staging_evidence_inflight_page (
    singleton                   INTEGER PRIMARY KEY CHECK (singleton = 1),
    previous_generation         INTEGER NOT NULL CHECK (previous_generation >= 0),
    previous_apply_receipt_digest BLOB NOT NULL CHECK (length(previous_apply_receipt_digest) = 32),
    generation                  INTEGER NOT NULL CHECK (generation > 0),
    operation_payload           BLOB NOT NULL CHECK (length(operation_payload) BETWEEN 1 AND 122880),
    page_digest                 BLOB NOT NULL CHECK (length(page_digest) = 32),
    apply_receipt               BLOB CHECK (apply_receipt IS NULL OR length(apply_receipt) BETWEEN 1 AND 4096),
    CHECK (generation = previous_generation + 1),
    CHECK (
        (previous_generation = 0 AND hex(previous_apply_receipt_digest) = '0000000000000000000000000000000000000000000000000000000000000000')
        OR (previous_generation > 0 AND hex(previous_apply_receipt_digest) != '0000000000000000000000000000000000000000000000000000000000000000')
    )
) STRICT;

PRAGMA user_version = 4;

COMMIT;
