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