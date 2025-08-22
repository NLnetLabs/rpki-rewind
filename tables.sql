CREATE TABLE events (
    id SERIAL NOT NULL,
    event TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    uri TEXT,
    hash TEXT,
    publication_point TEXT,
    PRIMARY KEY(id)
);

