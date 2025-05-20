#################################################################
# Flag domestication analysis by port flag
# comparing domestic port visit ratio before 2017 and after 2017
#################################################################

CREATE TEMP FUNCTION start_date () AS (TIMESTAMP "2012-01-01");
CREATE TEMP FUNCTION end_date () AS (TIMESTAMP "2022-01-01");
CREATE TEMPORARY FUNCTION psma_flag(flag STRING) AS ((
  SELECT
    flag IN (SELECT iso3 FROM `scratch_jaeyoon.psma_ratifiers_v20240815`)
));

CREATE TEMPORARY FUNCTION group_eu_flags(flag STRING) AS ((
  SELECT
    IF (flag IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'),
      "EU", flag) AS flag_eu
));

-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_port_visits_2016_2021_reflagged_vessels_bis2_v20230715` AS
CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_port_visits_by_reflagged_vessels_v20230715` AS

WITH
  ----------------
  -- Source tables
  ----------------
  source_reflagging_info AS (
    SELECT *
    FROM `world-fishing-827.vessel_identity.reflagging_core_all_v20220701`
  ),

  source_port_visits AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.product_events_port_visit`
  ),

  source_anchorages AS (
    SELECT *
    FROM `world-fishing-827.anchorages.named_anchorages_v20240117`
  ),

  source_vessel_info AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240501`
  ),

  source_identity_stitcher AS (
    SELECT *
    FROM `world-fishing-827.scratch_jaeyoon.flag_changes_identity_stitcher_v20220701`
  ),

  source_name_stitcher AS (
    SELECT *
    FROM `world-fishing-827.scratch_jaeyoon.flag_changes_name_stitcher_v20220701`
  ),

  ----------------------------------------------
  -- AIS-registry matched vessels that reflagged
  ----------------------------------------------
  reflagging AS (
    SELECT
      vessel_record_id, ssvid, n_shipname, n_callsign, imo,
      flag, flag_eu, first_timestamp, last_timestamp
    FROM (
      SELECT
        vessel_record_id, n_shipname, n_callsign,
        IF (vessel_record_id LIKE "%IMO-%",
          SPLIT (SPLIT (vessel_record_id, "IMO-")[OFFSET(1)], "|")[OFFSET(0)],
          NULL) AS imo,
        flag, flag_eu,
        first_timestamp, last_timestamp, num_events, is_fishing, is_carrier, is_bunker,
        SPLIT (ssvids_associated, "|") AS ssvid
      FROM source_reflagging_info
      WHERE is_fishing
        OR is_carrier )
    LEFT JOIN UNNEST (ssvid) AS ssvid
  ),

  ------------------
  -- Port visit data
  ------------------
  port_visits_raw AS (
    SELECT event_id, event_start,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag,
      JSON_EXTRACT_SCALAR (event_info, "$.intermediate_anchorage.anchorage_id") AS anchorage_id,
    FROM source_port_visits
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 3
      AND TIMESTAMP_DIFF (event_end, event_start, HOUR) > 3
  ),

  ------------------------------------------------------
  -- anchorage ids that represent unlikely landing ports
  ------------------------------------------------------
  canal_ids AS (
    SELECT s2id AS anchorage_id, label, sublabel, iso3
    FROM source_anchorages
    WHERE sublabel = "PANAMA CANAL"
      OR label = "SUEZ"
      OR label = "SUEZ CANAL"
      OR label = "SINGAPORE"
  ),

  -----------------------
  -- Filtered port visits
  -----------------------
  port_visits AS (
    SELECT *
    FROM port_visits_raw
    WHERE anchorage_id NOT IN (SELECT anchorage_id FROM canal_ids)
  ),

  -------------------------------
  -- AIS-registry matched vessels
  -------------------------------
  in_identity_dataset AS (
    SELECT ssvid, IFNULL (imo, "NULL") AS imo, flag_eu, vessel_record_id
    FROM (
      SELECT
        SPLIT (ssvids_associated, "|") AS ssvids,
        SPLIT (SPLIT (vessel_record_id, "IMO-")[SAFE_OFFSET(1)], "|")[OFFSET(0)] imo,
        flag_eu, vessel_record_id
      FROM source_reflagging_info )
    LEFT JOIN UNNEST (ssvids) AS ssvid
  ),

  ---------------------------------
  -- Registry-unmatched AIS vessels
  ---------------------------------
  from_unmatched_ais AS (
    SELECT
      "AIS_based_" || "IMO-" || imo AS vessel_record_id,
      ssvid, n_shipname, n_callsign, imo,
      flag, first_timestamp, last_timestamp
    FROM (
      SELECT
        ssvid,
        best.best_flag AS flag,
        -- ais_identity.n_imo_mostcommon.value AS imo,
        (SELECT value FROM UNNEST (ais_identity.n_imo) WHERE value IS NOT NULL ORDER BY count DESC, value ASC LIMIT 1) AS imo,
        ais_identity.n_shipname_mostcommon.value AS n_shipname,
        ais_identity.n_callsign_mostcommon.value AS n_callsign,
        activity.first_timestamp, activity.last_timestamp
      FROM source_vessel_info
      WHERE (on_fishing_list_best
          OR best.best_vessel_class IN ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory') ) )
    WHERE imo IS NOT NULL
      AND (ssvid, IFNULL (imo, "NULL")) NOT IN (SELECT (ssvid, imo) FROM in_identity_dataset)
      AND imo != "4194304"
    -- ORDER BY imo, first_timestamp
  ),

  -------------------------------------------------------------------------------
  -- Vessels associated with the same hulls based on identity stitcher algorithms
  -------------------------------------------------------------------------------
  assign_id_to_stitcher AS (
    SELECT *,
      IF (imo IS NOT NULL OR pair_imo IS NOT NULL, 'AIS_based_IMO-' || IFNULL (imo, pair_imo),
        'AIS_based_Stitcher-' || ssvid || '-' || pair_ssvid) AS vessel_record_id
    FROM source_identity_stitcher
    WHERE imo IS NULL OR pair_imo IS NULL
  ),

  from_identity_stitcher AS (
    SELECT
      vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag, first_timestamp, last_timestamp
    FROM assign_id_to_stitcher
    UNION DISTINCT
    SELECT
      vessel_record_id,
      pair_ssvid AS ssvid,
      pair_n_shipname AS n_shipname,
      pair_n_callsign AS n_callsign,
      pair_imo AS imo, pair_flag AS flag,
      pair_first_timestamp AS first_timestamp,
      pair_last_timestamp AS last_timestamp
    FROM assign_id_to_stitcher
  ),

  --------------------------------------------------------------------------
  -- Vessels associated with the same hulls based on name stitcher algorithm
  --------------------------------------------------------------------------
  from_name_stitcher AS (
    SELECT vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag, first_timestamp, last_timestamp
    FROM source_name_stitcher
  ),

  -----------------
  -- Group EU flags
  -----------------
  eu_grouping AS (
    SELECT
      *,
      group_eu_flags (flag) AS flag_eu
    FROM (
      SELECT
        vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag,
        MIN (first_timestamp) AS first_timestamp,
        MAX (last_timestamp) AS last_timestamp
      FROM (
        SELECT * FROM from_unmatched_ais
        UNION DISTINCT
        SELECT * FROM from_identity_stitcher
        UNION DISTINCT
        SELECT * FROM from_name_stitcher )
      GROUP BY 1,2,3,4,5,6 )
  ),

  ---------------------------------------------------------
  -- Select only the (unmatched) vessels that changed flags
  ---------------------------------------------------------
  multi_flags AS (
    SELECT
      vessel_record_id,
      ssvid, n_shipname, n_callsign, imo,
      flag, flag_eu, first_timestamp, last_timestamp
    FROM (
      SELECT *, COUNT (DISTINCT flag) OVER (PARTITION BY vessel_record_id) AS cnt_flags
      FROM eu_grouping )
    WHERE cnt_flags > 1
      AND vessel_record_id NOT LIKE "%IMO-1234567%"
  ),

  -----------------------------------------------------------------------------------------------------------
  -- Combined matched and unmatched vessels, and filter port visits that took place within vessel time ranges
  -----------------------------------------------------------------------------------------------------------
  combined AS (
    SELECT DISTINCT
      vessel_record_id, ssvid, shipname, n_shipname, n_callsign, imo,
      port_flag, flag, flag_eu,
      first_timestamp, last_timestamp
    FROM (
      SELECT * FROM reflagging UNION ALL
      SELECT * FROM multi_flags )
    LEFT JOIN port_visits
    USING (ssvid)
    WHERE event_start BETWEEN TIMESTAMP_ADD (first_timestamp, INTERVAL 14 DAY) AND TIMESTAMP_SUB (last_timestamp, INTERVAL 14 DAY)
  ),

  countings AS (
    SELECT *,
      COUNT (DISTINCT flag_eu) OVER (PARTITION BY vessel_record_id, port_flag) AS shift_in_eu
    FROM (
      SELECT *,
          COUNTIF (port_flag = flag) OVER (PARTITION BY vessel_record_id, port_flag) AS domestic_reflagging
      FROM (
        SELECT *,
          COUNT (DISTINCT flag) OVER (PARTITION BY vessel_record_id) AS flags_to_same_port
        FROM combined )
      -- WHERE flags_to_same_port < 2
      -- WHERE flags_to_same_port > 1 AND port_flag IS NOT NULL
      )
    -- WHERE domestic_reflagging > 0
  ),

  ---------------------------------------------------------------------------------
  -- Remove the vessels that change flags within EU only and get the last flag info
  ---------------------------------------------------------------------------------
  categories AS (
    SELECT * EXCEPT (flags_to_same_port, domestic_reflagging, shift_in_eu)
    FROM (
      SELECT *,
        FIRST_VALUE (flag) OVER (
          PARTITION BY vessel_record_id, port_flag
          ORDER BY first_timestamp DESC, last_timestamp DESC) AS last_flag,
      FROM countings
      WHERE shift_in_eu != 1 )
      ORDER BY vessel_record_id, first_timestamp, last_timestamp
  ),

  psma_port AS (
    SELECT * EXCEPT (last_flag),
      port_flag = last_flag AS domestic_reflagging,
      psma_flag (port_flag) AS psma,
      group_eu_flags (port_flag) AS port_flag_eu
    FROM categories
  ),

  ---------------------------------------
  -- This is only for the display purpose
  ---------------------------------------
  add_focus_port AS (
    SELECT * EXCEPT(focus_port_flag),
      ARRAY_TO_STRING (
        udfs.dedup_array (
          ARRAY_AGG (focus_port_flag)
            OVER (PARTITION BY vessel_record_id)),
        "|") AS focus_port_flag
    FROM (
      SELECT *, IF (flag_eu = port_flag_eu, port_flag_eu, NULL) AS focus_port_flag
      FROM psma_port )
  ),

  -------------------------------
  -- Some manual hard corrections
  -------------------------------
  corrections AS (
    SELECT * EXCEPT (first_timestamp),
      CASE
        WHEN vessel_record_id = "AIS_based_IMO-9699579"
        THEN TIMESTAMP "2018-08-15 21:45:32+00:00"
        WHEN vessel_record_id = "FFA-36608|IMO-9699567|WCPFC-11163"
        THEN TIMESTAMP "2018-08-17 01:40:23+00:00"
        ELSE first_timestamp
      END AS first_timestamp
    FROM add_focus_port
    WHERE vessel_record_id NOT IN (
      # False flag change alerts for Taiwan vessels
      'AIS_based_Stitcher-200006071-416006056',
      'AIS_based_Stitcher-200011842-416006881',
      'AIS_based_Stitcher-413588168-416588168-416042312',
      'AIS_based_Stitcher-413883946-416883946',
      'AIS_based_Stitcher-413883947-416883947',
      'AIS_based_Stitcher-416031500-441603150',
      'AIS_based_Stitcher-900030719-416054200',
      'IMO-8684864|NPFC-2'
    )
    QUALIFY COUNTIF (flag_eu = port_flag_eu) OVER (PARTITION BY vessel_record_id, port_flag_eu) > 0
  ),

  ---------------------------------------------------
  -- Add port visits to the vessels that change flags
  ---------------------------------------------------
  attach_port_visits AS (
    SELECT DISTINCT
      * EXCEPT (port_flag),
      group_eu_flags (port_flag) AS port_flag_eu,
    FROM (
      SELECT vessel_record_id, corrections.ssvid, n_shipname, n_callsign, imo, flag, flag_eu, first_timestamp, last_timestamp,
        IF (focus_port_flag LIKE "%|%", port_flag_eu, focus_port_flag) AS focus_port_flag,
        domestic_reflagging, psma,
        EXTRACT (YEAR FROM event_start) || "-" ||
          FORMAT ("%02d", EXTRACT (MONTH FROM event_start)) AS event_month, event_start, port_visits.port_flag
      FROM corrections
      LEFT JOIN port_visits
      ON (corrections.ssvid = port_visits.ssvid
        AND focus_port_flag LIKE "%" || group_eu_flags(port_visits.port_flag) || "%" )
      WHERE event_start BETWEEN TIMESTAMP_ADD (first_timestamp, INTERVAL 14 DAY) AND TIMESTAMP_SUB (last_timestamp, INTERVAL 14 DAY)
    )
    ORDER BY vessel_record_id, first_timestamp, event_month
  ),

  -------------------------------------------------------------------------------------------------------------------------------
  -- Add some minimum threshold: a vessel visiting a given port at least twice, a port receiving visits by at least three vessels
  -------------------------------------------------------------------------------------------------------------------------------
  additional_filter AS (
    SELECT *, COUNT (DISTINCT vessel_record_id) OVER (PARTITION BY port_flag_eu) AS total_num_vessels
    FROM (
      SELECT *,
      FROM (
        SELECT *,
          COUNT (DISTINCT event_start) OVER (
            PARTITION BY vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag_eu, port_flag_eu) AS multi_visits,
        FROM attach_port_visits )
      QUALIFY LOGICAL_AND (multi_visits > 1) OVER (PARTITION BY vessel_record_id) )
    #
    # At least 3 vessels that visited the given port
    QUALIFY COUNT (DISTINCT vessel_record_id) OVER (PARTITION BY port_flag_eu) > 2
  ),

  -----------------------------
  -- Calculate the yearly stats
  -----------------------------
  change_by_flag AS (
    SELECT port_flag_eu, event_year, cnt, year_total, cnt_vessels, total_num_vessels, ROUND (cnt / year_total, 2) AS ratio
    FROM (
      SELECT *, SUM (cnt) OVER (PARTITION BY event_year, port_flag_eu) AS year_total,
        -- SUM (cnt_vessels) OVER (PARTITION BY event_year, port_flag_eu) AS year_total_vessels
      FROM (
        SELECT
          is_target_flag,
          port_flag_eu,
          event_year,
          total_num_vessels,
          COUNT (DISTINCT ssvid) AS cnt_vessels,
          COUNT (DISTINCT event_start) AS cnt
        FROM (
          SELECT DISTINCT
            flag_eu = port_flag_eu AS is_target_flag,
            ssvid, flag_eu, port_flag_eu, event_year, event_start, total_num_vessels
          FROM (
            SELECT DISTINCT
              ssvid, flag_eu, port_flag_eu, event_start, total_num_vessels,
              -- EXTRACT (YEAR FROM event_start) AS event_year
              #
              # Before and after 2017 (noted 2016 vs. 2021 just for convenience)
              IF (event_start < '2017-01-01', 2016, 2021) AS event_year
            FROM additional_filter )
          WHERE event_year IN (2016, 2021) )
        GROUP BY 1,2,3,4 ) )
    WHERE is_target_flag
    ORDER BY 1,2
  )

#
# Ratio change by port flag before and after 2017
SELECT *,
  ROUND (SUM ( IF(event_year = 2021, ratio, -1 * ratio) ) OVER (PARTITION BY port_flag_eu), 2) AS diff,
  SUM (year_total) OVER (PARTITION BY port_flag_eu) AS total,
  psma_flag (port_flag_eu) AS is_psma
FROM change_by_flag
QUALIFY COUNT (*) OVER (PARTITION BY port_flag_eu) > 1
  AND LOGICAL_AND (year_total > 10) OVER (PARTITION BY port_flag_eu)
  AND LOGICAL_AND (cnt_vessels > 0) OVER (PARTITION BY port_flag_eu)
ORDER BY diff DESC, total DESC, port_flag_eu, event_year


--## 30% increase 14% decrease
--#
--# Ratio change by port flag before and after 2017
--SELECT is_psma, event_year, SUM (cnt) / SUM (year_total)
--FROM (
--SELECT *,
--  ROUND (SUM ( IF(event_year = 2021, ratio, -1 * ratio) ) OVER (PARTITION BY port_flag_eu), 2) AS diff,
--  SUM (year_total) OVER (PARTITION BY port_flag_eu) AS total,
--  psma_flag (port_flag_eu) AS is_psma
--FROM change_by_flag
--QUALIFY COUNT (*) OVER (PARTITION BY port_flag_eu) > 1
--  AND LOGICAL_AND (year_total > 10) OVER (PARTITION BY port_flag_eu)
--  AND LOGICAL_AND (cnt_vessels > 0) OVER (PARTITION BY port_flag_eu)
--ORDER BY diff DESC, total DESC, port_flag_eu, event_year )
--GROUP BY 1,2
--ORDER BY 1,2
