##################################################################
# SQL script to identify identities associated with the same hulls
# using ship name with the same flag
##################################################################

CREATE TEMP FUNCTION check_timestamp_overlap (
    arr ARRAY<STRUCT<first_timestamp TIMESTAMP, last_timestamp TIMESTAMP>>) AS ((
  WITH
    ------------------------------------
    -- Flatten the given array of struct
    ------------------------------------
    ts AS (
      SELECT first_timestamp, last_timestamp
      FROM UNNEST (arr)
    ),

    --------------------------------------------------------
    -- Cross join all time ranges except themselves
    -- to determine there is an overlap of time
    -- Give a certain length of days to tolerate transitions
    --------------------------------------------------------
    compare_ts AS (
      SELECT
        -------------------------------------------
        -- An overlap of up to 7 days is acceptable
        -------------------------------------------
        a.first_timestamp < TIMESTAMP_SUB (b.last_timestamp, INTERVAL 7 DAY)
        AND a.last_timestamp > TIMESTAMP_ADD (b.first_timestamp, INTERVAL 7 DAY) AS overlap
      FROM ts AS a
      CROSS JOIN ts AS b
      -----------------------------
      -- Avoid comparing themselves
      -----------------------------
      WHERE NOT (a.first_timestamp = b.first_timestamp
        AND a.last_timestamp = b.last_timestamp)
    )

  -------------------------------------------------------------------
  -- If only one time range per vessel is given, there is no overlap.
  -- Otherwise, determine by Logical OR if there is an overlap
  -------------------------------------------------------------------
  SELECT
    IF (COUNT (*) <= 1,
      FALSE,
      IF (LOGICAL_OR (overlap) IS NULL,
        TRUE, LOGICAL_OR (overlap) ) )
  FROM compare_ts
));

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.flag_changes_name_stitcher_v20220701` AS

WITH
  ---------------
  -- Source table
  ---------------
  source_vessel_info AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240501`
  ),

  vi AS (
    SELECT
      ssvid,
      ais_identity.n_shipname_mostcommon.value AS n_shipname,
      ais_identity.n_callsign_mostcommon.value AS n_callsign,
      ais_identity.n_imo_mostcommon.value AS imo,
      best.best_flag AS flag,
      best.best_vessel_class AS geartype,
      activity.first_timestamp, activity.last_timestamp,
      on_fishing_list_best AS is_fishing,
      best.best_vessel_class IN (
        "reefer", "specialized_reefer", "container_reefer",
        "well_boat", "fish_factory", "fish_tender") AS is_carrier
    FROM source_vessel_info
    WHERE on_fishing_list_best
      OR best.best_vessel_class IN ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory')
  ),

  name_filter AS (
    SELECT *,
    FROM (
      SELECT
        n_shipname,
        ARRAY_AGG (
          STRUCT (
            ssvid, flag, n_callsign, imo, geartype, is_fishing, is_carrier, first_timestamp, last_timestamp)
          ORDER BY first_timestamp, last_timestamp) name_group,
        check_timestamp_overlap (ARRAY_AGG (STRUCT (first_timestamp, last_timestamp))) AS timeoverlap,
        COUNT (DISTINCT flag) AS cnt_flag,
        COUNT (DISTINCT imo) AS cnt_imo,
        COUNTIF (imo IS NULL) AS cnt_null_imo,
        STRING_AGG (DISTINCT imo, "|") AS imos,
        LOGICAL_AND (is_fishing) AS is_fishing_all,
        LOGICAL_AND (is_carrier) AS is_carrier_all
      FROM vi
      WHERE is_fishing OR is_carrier
      GROUP BY 1 )
    WHERE NOT timeoverlap
      AND ARRAY_LENGTH (name_group) > 1
      AND cnt_flag > 1
      AND cnt_imo <= 1
      AND cnt_null_imo > 0
      AND (is_fishing_all OR is_carrier_all)
    ORDER BY ARRAY_LENGTH (name_group) DESC
  ),

  assign_uvi AS (
    SELECT *,
      IF (cnt_imo = 1,
        "AIS_based-IMO-" || imos,
        "AIS_based_Stitcher-" || ARRAY_TO_STRING (ARRAY (SELECT ssvid FROM UNNEST (name_group)), "-")) AS vessel_record_id
    FROM name_filter
  )

SELECT vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag, first_timestamp, last_timestamp, is_fishing_all, is_carrier_all
FROM assign_uvi
LEFT JOIN UNNEST (name_group)