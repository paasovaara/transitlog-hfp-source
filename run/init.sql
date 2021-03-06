CREATE ROLE creator LOGIN CREATEDB CREATEROLE;
\password creator;



SET ROLE creator;

CREATE ROLE hfp_writer LOGIN;
\password hfp_writer;

CREATE ROLE postgraphile LOGIN;
\password postgraphile;

CREATE ROLE hasura LOGIN;
\password hasura;

CREATE database vehicles;

RESET ROLE;



\c vehicles;

CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pgcrypto;



SET ROLE creator;

CREATE TYPE JOURNEY_TYPE AS ENUM ('journey', 'deadrun');
CREATE TYPE TRANSPORT_MODE AS ENUM ('bus', 'train', 'tram', 'metro', 'ferry');

CREATE TABLE vehicles (
    received_at        TIMESTAMPTZ      NOT NULL,
    topic_prefix       TEXT             NOT NULL,
    topic_version      TEXT             NOT NULL,
    journey_type       JOURNEY_TYPE     NOT NULL,
    is_ongoing         BOOLEAN          NOT NULL,
    mode               TRANSPORT_MODE   NOT NULL,
    owner_operator_id  SMALLINT         NOT NULL,
    vehicle_number     INTEGER          NOT NULL,
    unique_vehicle_id  TEXT             NOT NULL,
    route_id           TEXT                 NULL,
    direction_id       SMALLINT             NULL,
    headsign           TEXT                 NULL,
    journey_start_time TIME                 NULL,
    next_stop_id       TEXT                 NULL,
    geohash_level      SMALLINT             NULL,
    topic_latitude     DOUBLE PRECISION     NULL,
    topic_longitude    DOUBLE PRECISION     NULL,
    desi               TEXT                 NULL,
    dir                SMALLINT             NULL,
    oper               SMALLINT             NULL,
    veh                INTEGER          NOT NULL,
    tst                TIMESTAMPTZ      NOT NULL,
    tsi                BIGINT           NOT NULL,
    spd                DOUBLE PRECISION     NULL,
    hdg                DOUBLE PRECISION     NULL,
    lat                DOUBLE PRECISION     NULL,
    long               DOUBLE PRECISION     NULL,
    acc                DOUBLE PRECISION     NULL,
    dl                 INTEGER              NULL,
    odo                DOUBLE PRECISION     NULL,
    drst               BOOLEAN              NULL,
    oday               DATE                 NULL,
    jrn                SMALLINT             NULL,
    line               SMALLINT             NULL,
    start              TIME                 NULL
);

SELECT create_hypertable('vehicles',
                         'received_at',
                         partitioning_column => 'unique_vehicle_id',
                         chunk_time_interval => interval '1 hour');

GRANT INSERT ON TABLE vehicles TO hfp_writer;
GRANT SELECT ON TABLE vehicles TO PUBLIC;

CREATE INDEX route_id_idx ON vehicles (route_id);
CREATE INDEX direction_id_idx ON vehicles (direction_id);
CREATE INDEX journey_start_time_idx ON vehicles (journey_start_time);
CREATE INDEX unique_vehicle_id_idx ON vehicles (unique_vehicle_id);
CREATE INDEX oday_idx ON vehicles USING brin (oday);
CREATE INDEX lat_idx ON vehicles (lat);
CREATE INDEX long_idx ON vehicles (long);

-- Set up Hasura.

CREATE SCHEMA IF NOT EXISTS hdb_catalog;
CREATE SCHEMA IF NOT EXISTS hdb_views;

GRANT ALL PRIVILEGES ON SCHEMA hdb_catalog TO hasura;
GRANT ALL PRIVILEGES ON SCHEMA hdb_views TO hasura;

GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO hasura;
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO hasura;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO hasura;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO hasura;

RESET ROLE;
