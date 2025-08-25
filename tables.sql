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
    visible_on BIGINT, 
    disappeared_on BIGINT, 
    hash TEXT, 
    uri TEXT, 
    publication_point TEXT,
    PRIMARY KEY(id)
)

CREATE INDEX IF NOT EXISTS idx_objects_uri ON objects (uri);
CREATE INDEX IF NOT EXISTS idx_objects_publication_point ON objects (publication_point);
CREATE INDEX IF NOT EXISTS idx_objects_visible_on ON objects (visible_on);
CREATE INDEX IF NOT EXISTS idx_objects_disappeared_on ON objects (disappeared_on);