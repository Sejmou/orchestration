from prefect.schedules import Schedule

from utils.flow_deployment import create_image_config
from flows.clickhouse.run_queries import run_queries, QueryMeta

# TODO: fetch code from data repo (atm, code is duplicated there) or find some other solution for deduplication of logic
_sql_code = """
/*
  Pt. 1: update tbl holding most streamed track IDs per ISRC
  NOTE: this would be a refreshable materialized view normally, but those are broken on ClickHouse on Kubernetes
*/
-- create temp table with new data (removing existing one from previous runs, if there is one)
DROP TABLE IF EXISTS spotify.tmp_data_most_streamed_track_id_per_isrc;
CREATE TABLE spotify.tmp_data_most_streamed_track_id_per_isrc
ORDER BY isrc
AS
SELECT isrc, argMax(track_id, streams) AS track_id, max(streams) AS track_id_streams FROM (
    SELECT isrc, track_id, sum(streams) AS streams
    FROM spotify.total_streams_de_at_ch_by_isrc_and_track_id
    GROUP BY isrc, track_id
)
GROUP BY isrc;

-- 'replace' the old table with the new temp table (without using the REPLACE TABLE syntax which is broken on ClickHouse on Kubernetes)
RENAME TABLE IF EXISTS spotify.data_most_streamed_track_id_per_isrc TO spotify.data_most_streamed_track_id_per_isrc_old;
RENAME TABLE spotify.tmp_data_most_streamed_track_id_per_isrc TO spotify.data_most_streamed_track_id_per_isrc;
DROP TABLE IF EXISTS spotify.data_most_streamed_track_id_per_isrc_old;

/*
  Pt. 2: update the tbl for Spotify track metadata per ISRC
  NOTE: again, this would be a refreshable materialized view normally, but those are broken on ClickHouse on Kubernetes
*/
-- create temp table for new data with specific schema (not possible when using CREATE TABLE AS SELECT ...)
DROP TABLE IF EXISTS spotify.tmp_data_isrc_track_meta_de;
CREATE TABLE spotify.tmp_data_isrc_track_meta_de (
    isrc  FixedString(12),
    id  String,
    relinked_to_id  Nullable(String),
    name  String,
    artists  Array(Tuple(
    id String,
    name String)),
    album  Tuple(
    id String,
    name String,
    album_type String,
    artists Array(Tuple(
        id String,
        name String)),
    release_date Nullable(Date32),
    release_date_precision Nullable(String),
    release_date_string String,
    total_tracks UInt16,
    images Array(Tuple(
        url String,
        height UInt16,
        width UInt16))),
    duration_ms  UInt32,
    explicit  Bool,
    is_playable  Nullable(Bool),
    popularity  UInt8,
    restriction  Nullable(String),
    track_number  UInt16,
    disc_number  UInt8,
    preview_url  Nullable(String),
    observed_at  DateTime
) ORDER BY isrc;

-- fill temp table with new data
INSERT INTO spotify.tmp_data_isrc_track_meta_de
SELECT
  s.isrc AS isrc,
  id,
  relinked_to_id,
  name,
  artists,
  album,
  duration_ms,
  explicit,
  is_playable,
  popularity,
  restriction,
  track_number,
  disc_number,
  preview_url,
  observed_at,
FROM spotify.data_track_id_meta_de t
JOIN spotify.data_most_streamed_track_id_per_isrc s
ON t.id = s.track_id;

-- insert ISRCs not existing in spotify.data_most_streamed_track_id_per_isrc
INSERT INTO spotify.tmp_data_isrc_track_meta_de
SELECT
  replaceRegexpAll(t.isrc, '[-\\s]', '') AS isrc,
  t.id,
  t.relinked_to_id,
  t.name,
  t.artists,
  t.album,
  t.duration_ms,
  t.explicit,
  t.is_playable,
  t.popularity,
  t.restriction,
  t.track_number,
  t.disc_number,
  t.preview_url,
  t.observed_at
FROM (
    SELECT
      replaceRegexpAll(isrc, '[-\\s]', '') AS isrc,
      -- pick the earliest ID; fall back to lexicographically first in case of a tie
      argMin(id, (observed_at, id)) AS id
    FROM spotify.data_track_id_meta_de
    WHERE length(isrc) = 12 AND isrc NOT IN (
      SELECT isrc FROM spotify.data_most_streamed_track_id_per_isrc
    )
    GROUP BY isrc
) earliest_not_in_data_most_streamed_track_id_per_isrc
JOIN spotify.data_track_id_meta_de t
  ON t.id = earliest_not_in_data_most_streamed_track_id_per_isrc.id;

-- 'replace' the old table with the new temp table (without using the REPLACE TABLE syntax which is broken on ClickHouse on Kubernetes)
RENAME TABLE IF EXISTS spotify.data_isrc_track_meta_de TO spotify.data_isrc_track_meta_de_old;
RENAME TABLE spotify.tmp_data_isrc_track_meta_de TO spotify.data_isrc_track_meta_de;
DROP TABLE IF EXISTS spotify.data_isrc_track_meta_de_old;
"""

if __name__ == "__main__":
    queries = [q.strip() for q in _sql_code.split(";")]
    query_templates = [
        QueryMeta(query_or_template=q).model_dump(mode="json") for q in queries if q
    ]
    run_queries.deploy(
        "K8S: spotify.data_isrc_track_meta_de + spotify.data_most_streamed_track_id_per_isrc",
        tags=["Spotify", "ClickHouse"],
        schedule=Schedule(
            cron="50 7 * * *",
            timezone="Europe/Berlin",
        ),
        parameters={
            "query_templates": query_templates,
            "server": "k8s",
        },
        work_pool_name="Docker",
        image=create_image_config("spotify-ch-k8s-isrc-track-meta-de", "v1.1"),
    )
