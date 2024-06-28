# Keywords: Identity stitcher to identify identities associated with the same hulls
---------------------------------
-- IDENTITY STITCHER GAP FILTERED
---------------------------------

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.flag_changes_identity_stitcher_v20220701` AS

WITH
  ----------------
  -- Source tables
  ----------------
  source_identity_stitcher_raw AS (
    SELECT *
    FROM `world-fishing-827.scratch_jaeyoon.identity_stitcher_raw_v20220701`
  ),

  source_vessel_info AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240501`
  ),

  -------------------------------------------------------------------------------------
  -- Filter only vessel identities that are likely associated with the same vessel hull
  -------------------------------------------------------------------------------------
  stitcher_gap_filter AS (
    SELECT
      pair_id, ssvid, pair_ssvid, flag, pair_flag, port_iso3,
      n_shipname, pair_n_shipname, n_callsign, pair_n_callsign, imo, pair_imo, geartype, pair_geartype,
      IF (port_label = "KAOSIUNG", "KAOHSIUNG", port_label) AS port_label,
      first_timestamp, last_timestamp, pair_first_timestamp, pair_last_timestamp,
      distance_gap_meter, time_gap_minute, num_paired_forward, rank_dist_forward, num_paired_backward, rank_dist_backward
    FROM source_identity_stitcher_raw
    WHERE pair_id NOT LIKE "%0|0%"
      AND ((imo = pair_imo)
        OR ((imo IS NULL OR pair_imo IS NULL) AND distance_gap_meter <= 30 AND rank_dist_forward = 1 AND rank_dist_backward = 1)
        OR (num_paired_forward = 1 AND num_paired_backward = 1))
  ),

  ---------------------
  -- Vessel information
  ---------------------
  vessel_info AS (
    SELECT ssvid, on_fishing_list_best, best.best_vessel_class
    FROM source_vessel_info
    WHERE on_fishing_list_best
      OR best.best_vessel_class IN ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory')
  ),

  ----------------------------------------------------------------------
  -- Select only the stitched identity sets with consistent vessel class
  ----------------------------------------------------------------------
  geartype_filter AS (
    SELECT *
    FROM (
      SELECT
        a.*,
        b.on_fishing_list_best AS is_fishing,
        c.on_fishing_list_best AS pair_is_fishing,
        udfs.is_carrier (b.best_vessel_class) is_carrier,
        udfs.is_carrier (c.best_vessel_class) pair_is_carrier
      FROM stitcher_gap_filter AS a
      LEFT JOIN vessel_info AS b
      ON a.ssvid = b.ssvid
      LEFT JOIN vessel_info AS c
      ON a.pair_ssvid = c.ssvid )
    WHERE ((is_fishing AND pair_is_fishing)
        OR (is_carrier AND pair_is_carrier))
  )

SELECT * EXCEPT (num_paired_forward, rank_dist_forward, num_paired_backward, rank_dist_backward)
FROM geartype_filter
WHERE flag IS NOT NULL
  AND pair_flag IS NOT NULL
