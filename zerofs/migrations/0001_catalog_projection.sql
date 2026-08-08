CREATE TABLE IF NOT EXISTS zerofs_catalog_projection_state (
    volume_id UUID PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    observed_generation BIGINT NOT NULL CHECK (observed_generation >= -1)
);

CREATE TABLE IF NOT EXISTS zerofs_catalog_projection_resources (
    volume_id UUID NOT NULL,
    resource_id UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('branch', 'checkpoint')),
    name TEXT NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'deleting', 'deleted', 'absent')),
    parent_id UUID,
    origin_checkpoint_id UUID,
    observed_generation BIGINT NOT NULL CHECK (observed_generation >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ,
    customer_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(customer_metadata) = 'object'),
    PRIMARY KEY (volume_id, resource_id)
);

CREATE INDEX IF NOT EXISTS zerofs_catalog_projection_volume_name_idx
    ON zerofs_catalog_projection_resources (volume_id, name);

CREATE INDEX IF NOT EXISTS zerofs_catalog_projection_parent_idx
    ON zerofs_catalog_projection_resources (volume_id, parent_id);
