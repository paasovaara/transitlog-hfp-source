CREATE KEYSPACE vehicles WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

USE vehicles;

-- no support for enums
-- CREATE TYPE JOURNEY_TYPE AS ENUM ('journey', 'deadrun');
-- CREATE TYPE TRANSPORT_MODE AS ENUM ('bus', 'train', 'tram', 'metro', 'ferry');

CREATE TABLE vehicles (
    id                 BIGINT PRIMARY KEY, --required in cassandra. we could also use multiple columns as pk
    received_at        TIMESTAMP,
    topic_prefix       TEXT,
    topic_version      TEXT,
    journey_type       TEXT,  -- no support for enums
    is_ongoing         BOOLEAN,
    mode               TEXT,  -- no support for enums
    owner_operator_id  SMALLINT,
    vehicle_number     INT,
    unique_vehicle_id  TEXT,
    route_id           TEXT,
    direction_id       INT,
    headsign           TEXT,
    journey_start_time TIME,
    next_stop_id       TEXT,
    geohash_level      SMALLINT,
    topic_latitude     DOUBLE,
    topic_longitude    DOUBLE,
    desi               TEXT,
    dir                SMALLINT,
    oper               SMALLINT,
    veh                INT,
    tst                TIMESTAMP,
    tsi                BIGINT,
    spd                DOUBLE,
    hdg                DOUBLE,
    lat                DOUBLE,
    long               DOUBLE,
    acc                DOUBLE,
    dl                 INT,
    odo                DOUBLE,
    drst               BOOLEAN,
    oday               DATE,
    jrn                SMALLINT,
    line               SMALLINT,
    start              TIME
);


--GRANT INSERT ON TABLE vehicles TO hfp_writer;
--GRANT SELECT ON TABLE vehicles TO PUBLIC;

CREATE INDEX route_id_idx ON vehicles (route_id);
CREATE INDEX direction_id_idx ON vehicles (direction_id);
CREATE INDEX journey_start_time_idx ON vehicles (journey_start_time);
CREATE INDEX unique_vehicle_id_idx ON vehicles (unique_vehicle_id);
CREATE INDEX oday_idx ON vehicles (oday);
CREATE INDEX lat_idx ON vehicles (lat);
CREATE INDEX long_idx ON vehicles (long);
