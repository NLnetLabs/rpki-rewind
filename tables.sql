CREATE TABLE events (
    id SERIAL NOT NULL,
    event TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    uri TEXT,
    hash TEXT,
    publication_point TEXT,
    PRIMARY KEY(id)
);

CREATE TABLE objects (
    id SERIAL NOT NULL,
    content BYTEA NOT NULL, 
    content_json JSONB,
    visible_on BIGINT, 
    disappeared_on BIGINT, 
    hash TEXT, 
    uri TEXT, 
    publication_point TEXT,
    PRIMARY KEY(id)
);

CREATE TABLE roas (
    id SERIAL NOT NULL,
    object_id INTEGER REFERENCES objects (id),
    prefix CIDR NOT NULL,
    max_length SMALLINT,
    as_id BIGINT NOT NULL,
    not_before TIMESTAMP NOT NULL,
    not_after TIMESTAMP NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE aspas (
    id SERIAL NOT NULL,
    object_id INTEGER REFERENCES objects (id),
    customer BIGINT NOT NULL,
    provider BIGINT NOT NULL,
    not_before TIMESTAMP NOT NULL,
    not_after TIMESTAMP NOT NULL,
    PRIMARY KEY(id)
);

CREATE INDEX IF NOT EXISTS idx_objects_uri ON objects (uri);
CREATE INDEX IF NOT EXISTS idx_objects_publication_point ON objects (publication_point);
CREATE INDEX IF NOT EXISTS idx_objects_visible_on ON objects (visible_on);
CREATE INDEX IF NOT EXISTS idx_objects_disappeared_on ON objects (disappeared_on);
CREATE INDEX IF NOT EXISTS idx_objects_content_json ON objects USING gin (content_json);

CREATE INDEX IF NOT EXISTS idx_roas_prefix ON roas (prefix);
CREATE INDEX IF NOT EXISTS idx_roas_as_id ON roas (as_id);

CREATE INDEX IF NOT EXISTS idx_aspas_customer ON aspas (customer);
CREATE INDEX IF NOT EXISTS idx_aspas_provider ON aspas (provider);