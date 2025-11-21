CREATE KEYSPACE if not exists DE_2025_datalake
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE DE_2025_datalake;

CREATE TABLE tracking (
    create_time timeuuid PRIMARY KEY,
    bid text,
    bn text,
    campaign_id text,
    cd text,
    custom_track text,
    de text,
    dl text,
    dt text,
    ed text,
    ev text,
    group_id text,
    id text,
    job_id text,
    md text,
    publisher_id text,
    rl text,
    sr text,
    ts text,
    tz text,
    ua text,
    uid text,
    utm_campaign text,
    utm_content text,
    utm_medium text,
    utm_source text,
    utm_term text,
    v text,
    vp text
);

SELECT * FROM de_2025_datalake.tracking;