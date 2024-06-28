------------------------------------------------------------------------------
-- IDENTITY STITCHER RAW
-- This query template helps produce a data table of
-- where vessels change identities by "stitching" vessels activity time ranges
-- combined with their first/last AIS positions of fishing/support vessels
-- (Warning): This query consumes about 2TB to run
--
-- Last update: 2024-01-01
------------------------------------------------------------------------------

-------------------------------------------------------------------
-- Allowed time gap and distance between two consecutive identities
-------------------------------------------------------------------
CREATE TEMP FUNCTION allowed_time_gap() AS (24 * 60);
CREATE TEMP FUNCTION allowed_distance_gap() AS (1000);
-----------------------------------------------------------------------
-- Allowed time gap between AIS identity messages and position messages
-----------------------------------------------------------------------
CREATE TEMP FUNCTION allowed_pipe_time_gap() AS (24 * 30);
-------------------------
-- Time range of interest
-------------------------
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2012-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2023-01-01");

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.identity_stitcher_raw_v20220701` AS

WITH
  ----------------
  -- Source tables
  ----------------
  source_vessel_info AS (
    SELECT *
    FROM `world-fishing-827.gfw_research.vi_ssvid_v20220701`
  ),

  source_ais_messages AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.messages`
  ),

  source_anchorages AS (
    SELECT *
    FROM `world-fishing-827.anchorages.named_anchorages_v20240117`
  ),

  ---------------------------------------------------------
  -- Vessel identities from the vessel identity dataset,
  -- Turn Null values to STRING to be able to JOIN properly
  ---------------------------------------------------------
  raw_data AS (
    SELECT
      ssvid,
      IFNULL (ais_identity.n_shipname_mostcommon.value, "NULL") AS n_shipname,
      IFNULL (ais_identity.n_callsign_mostcommon.value, "NULL") AS n_callsign,
      IFNULL (ais_identity.n_imo_mostcommon.value, "NULL") AS imo,
      IFNULL (best.best_flag, "NULL") AS flag,
      best.best_vessel_class AS geartype,
      activity.first_timestamp, activity.last_timestamp,
      on_fishing_list_best AS is_fishing,
      best.best_vessel_class IN (
        "reefer", "specialized_reefer", "container_reefer",
        "well_boat", "fish_factory", "fish_tender") AS is_carrier
    FROM source_vessel_info
    WHERE activity.first_timestamp IS NOT NULL
      AND activity.last_timestamp IS NOT NULL
      #
      # At least 7 day worth of activity per an identity
      AND TIMESTAMP_DIFF (activity.last_timestamp, activity.first_timestamp, SECOND) > 60 * 60 * 24 * 7
  ),

  ------------------------------------
  -- Positional messages from pipeline
  ------------------------------------
  location_info AS (
    SELECT ssvid, lat, lon, timestamp, DATE(timestamp) AS date, distance_from_port_m
    FROM source_ais_messages
    WHERE timestamp >= start_date()
      AND timestamp < end_date()
      AND timestamp IS NOT NULL
      AND lat BETWEEN -90 AND 90
  ),

  ---------------------------------------------------------------------------
  -- Find the most likely position of the first timestamp of vessels
  ---------------------------------------------------------------------------
  id_pairing_spatial_match_start_point AS (
    SELECT
      * EXCEPT (min_first_timestamp_gap),
      ST_GEOGPOINT (first_position_lon, first_position_lat) AS first_position
    FROM (
      SELECT
        *,
        MIN (first_timestamp_gap) OVER (PARTITION BY ssvid) AS min_first_timestamp_gap
      FROM (
        SELECT
          a.*,
          b.lon AS first_position_lon, b.lat AS first_position_lat,
          TIMESTAMP_DIFF (b.timestamp, a.first_timestamp, SECOND) AS first_timestamp_gap,
          b.distance_from_port_m AS first_distance_port,
        FROM raw_data AS a
        LEFT JOIN location_info AS b
        ON (a.ssvid = b.ssvid
          ------------------------------------------------------------------------------------------
          -- There may be some gaps between first/last timestamp and pipeline timestamps
          -- because of lack of identity messages in some cases (only positional messages available)
          -- For these cases, allow such gaps to be within a designated time range (i.e. a month)
          ------------------------------------------------------------------------------------------
          AND ABS (TIMESTAMP_DIFF (a.first_timestamp, b.timestamp, HOUR)) < 24 * 30 ) ) )
    -----------------------------------------------------------
    -- Find the pipeline position messages
    -- going further in the past (within the allowed time gap)
    -----------------------------------------------------------
    WHERE ( min_first_timestamp_gap = first_timestamp_gap
      OR min_first_timestamp_gap IS NULL )
  ),

  ---------------------------------------------------------------------------
  -- Find the most likely position of the last timestamp of vessels
  ---------------------------------------------------------------------------
  id_pairing_spatial_match_end_point AS (
    SELECT
      * EXCEPT (max_last_timestamp_gap),
      ST_GEOGPOINT (last_position_lon, last_position_lat) AS last_position,
    FROM (
      SELECT
        *,
        MAX (last_timestamp_gap) OVER (PARTITION BY ssvid) AS max_last_timestamp_gap,
      FROM (
        SELECT
          a.*,
          c.lon AS last_position_lon, c.lat AS last_position_lat,
          TIMESTAMP_DIFF (c.timestamp, a.last_timestamp, SECOND) AS last_timestamp_gap,
          c.distance_from_port_m AS last_distance_port
        FROM id_pairing_spatial_match_start_point AS a
        LEFT JOIN location_info AS c
        ON (a.ssvid = c.ssvid
          AND ABS (TIMESTAMP_DIFF (a.last_timestamp, c.timestamp, HOUR)) < 24 * 30 ) ) )
    -----------------------------------------------------------
    -- Find the pipeline position messages
    -- going further in the future (within the allowed time gap)
    -----------------------------------------------------------
    WHERE ( max_last_timestamp_gap = last_timestamp_gap
      OR max_last_timestamp_gap IS NULL )
  ),

  -----------------------------------------------------------------------------------------------
  -- Plug vessels based on the following rules:
  -- 1. first_timestamp of one vessel and last_timestamp of another vessel is within preset hours,
  -- 2. These vessels are close enough, within preset km at the point of their first or last timestamp
  -- 3. Note the distance from port at the time of "connection."
  -----------------------------------------------------------------------------------------------
  id_pairing AS (
    SELECT
      ssvid || "|" || pair_ssvid || "|" || CAST (DATE (last_timestamp) AS STRING) || "|" ||
      ROUND (last_position_lat, 2) || "|" || ROUND (last_position_lon, 2) AS pair_id,
      *,
      IFNULL (next_distance, prev_distance) AS distance_gap_meter
    FROM (
      SELECT
        IF (a.last_timestamp < b.first_timestamp, a.ssvid, b.ssvid) AS ssvid,
        IF (a.last_timestamp < b.first_timestamp, a.n_shipname, b.n_shipname) AS n_shipname,
        IF (a.last_timestamp < b.first_timestamp, a.n_callsign, b.n_callsign) AS n_callsign,
        IF (a.last_timestamp < b.first_timestamp, a.imo, b.imo) AS imo,
        IF (a.last_timestamp < b.first_timestamp, a.flag, b.flag) AS flag,
        IF (a.last_timestamp < b.first_timestamp, a.geartype, b.geartype) AS geartype,
        IF (a.last_timestamp < b.first_timestamp, a.is_fishing, b.is_fishing) AS is_fishing,
        IF (a.last_timestamp < b.first_timestamp, a.is_carrier, b.is_carrier) AS is_carrier,
        IF (a.last_timestamp < b.first_timestamp, a.first_timestamp, b.first_timestamp) AS first_timestamp,
        IF (a.last_timestamp < b.first_timestamp, a.last_timestamp, b.last_timestamp) AS last_timestamp,

        IF (a.last_timestamp < b.first_timestamp, b.ssvid, a.ssvid) AS pair_ssvid,
        IF (a.last_timestamp < b.first_timestamp, b.n_shipname, a.n_shipname) AS pair_n_shipname,
        IF (a.last_timestamp < b.first_timestamp, b.n_callsign, a.n_callsign) AS pair_n_callsign,
        IF (a.last_timestamp < b.first_timestamp, b.imo, a.imo) AS pair_imo,
        IF (a.last_timestamp < b.first_timestamp, b.flag, a.flag) AS pair_flag,
        IF (a.last_timestamp < b.first_timestamp, b.geartype, a.geartype) AS pair_geartype,
        IF (a.last_timestamp < b.first_timestamp, b.is_fishing, a.is_fishing) AS pair_is_fishing,
        IF (a.last_timestamp < b.first_timestamp, b.is_carrier, a.is_carrier) AS pair_is_carrier,
        IF (a.last_timestamp < b.first_timestamp, b.first_timestamp, a.first_timestamp) AS pair_first_timestamp,
        IF (a.last_timestamp < b.first_timestamp, b.last_timestamp, a.last_timestamp) AS pair_last_timestamp,

        IF (a.last_timestamp < b.first_timestamp, a.last_distance_port, b.last_distance_port) AS last_distance_port,
        IF (a.last_timestamp < b.first_timestamp, a.last_position_lat, b.last_position_lat) AS last_position_lat,
        IF (a.last_timestamp < b.first_timestamp, a.last_position_lon, b.last_position_lon) AS last_position_lon,
        IF (a.last_timestamp < b.first_timestamp, b.first_distance_port, a.first_distance_port) AS first_distance_port,
        IF (a.last_timestamp < b.first_timestamp, b.first_position_lat, a.first_position_lat) AS first_position_lat,
        IF (a.last_timestamp < b.first_timestamp, b.first_position_lon, a.first_position_lon) AS first_position_lon,
        ( TIMESTAMP_DIFF (b.first_timestamp, a.last_timestamp, HOUR) BETWEEN 0 AND allowed_time_gap()
          AND ST_DISTANCE (b.first_position, a.last_position) < allowed_distance_gap() ) AS is_next_identity,
        ST_DISTANCE (b.first_position, a.last_position) AS next_distance,
        ( TIMESTAMP_DIFF (a.first_timestamp, b.last_timestamp, HOUR) BETWEEN 0 AND allowed_time_gap()
          AND ST_DISTANCE (a.first_position, b.last_position) < allowed_distance_gap() ) AS is_prev_identity,
        ST_DISTANCE (a.first_position, b.last_position) AS prev_distance
      FROM id_pairing_spatial_match_end_point AS a
      JOIN id_pairing_spatial_match_end_point AS b
      ON (TIMESTAMP_DIFF (b.first_timestamp, a.last_timestamp, HOUR) BETWEEN 0 AND allowed_time_gap())
        OR (TIMESTAMP_DIFF (a.first_timestamp, b.last_timestamp, HOUR) BETWEEN 0 AND allowed_time_gap())  )
    WHERE (is_next_identity OR is_prev_identity)
      AND first_timestamp != pair_first_timestamp
      AND last_timestamp != pair_last_timestamp
      AND last_position_lat != 0
      AND last_position_lon != 0
      AND first_position_lat != 0
      AND first_position_lon != 0
  ),

  ---------------------------------------------------------------------------------------------------
  -- In case there are multiple matches between vessels (one's last and the other's first timestamps)
  -- get ranking of these matches by distance between the vessel pairs
  ---------------------------------------------------------------------------------------------------
  multi_match_rank AS (
    SELECT
      *,
      COUNT (*) OVER (
        PARTITION BY ssvid, n_shipname, n_callsign, imo, flag) AS num_paired_forward,
      RANK () OVER (
        PARTITION BY ssvid, n_shipname, n_callsign, imo, flag
        ORDER BY distance_gap_meter ASC) AS rank_dist_forward,
      NULL AS num_paired_backward,
      NULL AS rank_dist_backward
    FROM id_pairing
    WHERE is_next_identity

    UNION ALL

    SELECT
      *,
      NULL AS num_paired_forward,
      NULL AS rank_dist_forward,
      COUNT (*) OVER (
        PARTITION BY pair_ssvid, pair_n_shipname, pair_n_callsign, pair_imo, pair_flag) AS num_paired_backward,
      RANK () OVER (
        PARTITION BY pair_ssvid, pair_n_shipname, pair_n_callsign, pair_imo, pair_flag
        ORDER BY distance_gap_meter ASC) AS rank_dist_backward,
    FROM id_pairing
    WHERE is_prev_identity
  ),

  --------------------------
  -- Pull ports' information
  --------------------------
  ports AS (
    SELECT
      lon, lat, ST_GEOGPOINT (lon, lat) AS port_pos,
      label AS port_label, iso3 AS port_iso3
    FROM source_anchorages
  ),

  -----------------------------------------------------------
  -- Combine all information about the identity change points
  -----------------------------------------------------------
  change_points AS (
    SELECT
      pair_id,
      num_paired_forward,
      rank_dist_forward,
      num_paired_backward,
      rank_dist_backward,
      distance_gap_meter,
      TIMESTAMP_DIFF (pair_first_timestamp, last_timestamp, MINUTE) AS time_gap_minute,
      first_position_lat,
      first_position_lon,
      first_timestamp,
      last_timestamp,
      pair_first_timestamp,
      pair_last_timestamp,
      IF (flag="NULL", NULL, flag) AS flag,
      IF (pair_flag="NULL", NULL, pair_flag) AS pair_flag,
      IF (imo="NULL", NULL, imo) AS imo,
      IF (pair_imo="NULL", NULL, pair_imo) AS pair_imo,
      ssvid,
      pair_ssvid,
      IF (n_shipname="NULL", NULL, n_shipname) AS n_shipname,
      IF (pair_n_shipname="NULL", NULL, pair_n_shipname) AS pair_n_shipname,
      IF (n_callsign="NULL", NULL, n_callsign) AS n_callsign,
      IF (pair_n_callsign="NULL", NULL, pair_n_callsign) AS pair_n_callsign,
      geartype,
      pair_geartype,
      is_fishing,
      is_carrier,
      pair_is_fishing,
      pair_is_carrier,
      first_distance_port,
      last_distance_port,
      is_prev_identity,
      is_next_identity
    FROM multi_match_rank
    -----------------------------------
    -- Deduplicate cases of A-B and B-A
    -----------------------------------
    -- WHERE is_next_identity
  ),

  -----------------------------------------------------
  -- Add port information to the identity change points
  -----------------------------------------------------
  add_ports AS (
    SELECT * EXCEPT (change_point_pos, port_pos, port_dist, port_rank, lon, lat)
    FROM (
      SELECT
        *,
        RANK () OVER (
          PARTITION BY pair_id
          ORDER BY port_dist ASC NULLS LAST) AS port_rank
      FROM (
        SELECT *, ST_DISTANCE (port_pos, change_point_pos) AS port_dist
        FROM (
          SELECT *, ST_GEOGPOINT (first_position_lon, first_position_lat) AS change_point_pos
          FROM change_points ) AS a
        LEFT JOIN ports AS b
        -----------------------------------------------------------------------------
        -- Allow 100th degree distance between the identity change point and the port
        -----------------------------------------------------------------------------
        ON a.first_position_lon BETWEEN b.lon - 0.01 AND b.lon + 0.01
          AND a.first_position_lat BETWEEN b.lat - 0.01 AND b.lat + 0.01) )
    WHERE port_rank = 1
  ),

  collapse AS (
    SELECT DISTINCT
      pair_id,
      MIN (distance_gap_meter) OVER (PARTITION BY pair_id) AS distance_gap_meter,
      MIN (time_gap_minute) OVER (PARTITION BY pair_id) AS time_gap_minute,
      MAX (num_paired_forward) OVER (PARTITION BY pair_id) num_paired_forward,
      MAX (rank_dist_forward) OVER (PARTITION BY pair_id) rank_dist_forward,
      MAX (num_paired_backward) OVER (PARTITION BY pair_id) num_paired_backward,
      MAX (rank_dist_backward) OVER (PARTITION BY pair_id) rank_dist_backward,
      * EXCEPT (pair_id, distance_gap_meter, time_gap_minute, num_paired_forward, rank_dist_forward, num_paired_backward, rank_dist_backward)
    FROM add_ports
  )

SELECT *
FROM collapse
