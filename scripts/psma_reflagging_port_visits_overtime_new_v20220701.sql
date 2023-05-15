CREATE TEMP FUNCTION start_date () AS (TIMESTAMP "2014-01-01");
CREATE TEMP FUNCTION end_date () AS (TIMESTAMP "2022-01-01");

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_reflagging_port_visits_overtime_new_v20220701` AS

WITH
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
      FROM `vessel_identity.reflagging_core_all_v20220701` )
    LEFT JOIN UNNEST (ssvid) AS ssvid
  ),

  port_visits AS (
    SELECT event_id, event_start,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag
    FROM `pipe_production_v20201001.published_events_port_visits`
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 2
  ),

  in_identity_dataset AS (
    SELECT ssvid, IFNULL (imo, "NULL") AS imo, flag_eu, vessel_record_id
    FROM (
      SELECT
        SPLIT (ssvids_associated, "|") AS ssvids,
        SPLIT (SPLIT (vessel_record_id, "IMO-")[SAFE_OFFSET(1)], "|")[OFFSET(0)] imo,
        flag_eu, vessel_record_id
      FROM `vessel_identity.reflagging_core_all_v20220701` )
    LEFT JOIN UNNEST (ssvids) AS ssvid
  ),

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
      FROM `gfw_research.vi_ssvid_v20220601`
      WHERE (on_fishing_list_best
          OR udfs.is_carrier (best.best_vessel_class) ) )
    WHERE imo IS NOT NULL
      AND (ssvid, IFNULL (imo, "NULL")) NOT IN (SELECT (ssvid, imo) FROM in_identity_dataset)
    -- ORDER BY imo, first_timestamp
  ),

  assign_id_to_stitcher AS (
    SELECT *,
      IF (imo IS NOT NULL OR pair_imo IS NOT NULL, 'AIS_based_IMO-' || IFNULL (imo, pair_imo),
        'AIS_based_Stitcher-' || ssvid || '-' || pair_ssvid) AS vessel_record_id
    FROM `scratch_jaeyoon.flag_changes_identity_stitcher_v20220701`
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

  from_name_stitcher AS (
    SELECT vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag, first_timestamp, last_timestamp
    FROM `scratch_jaeyoon.flag_changes_name_stitcher_v20220701`
  ),

  -----------------
  -- Group EU flags
  -----------------
  eu_grouping AS (
    SELECT
      *,
      IF (flag IN (
          'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
          'CZE', 'DNK', 'EST', 'FIN', 'FRA',
          'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
          'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
          'POL', 'PRT', 'ROU', 'SVK', 'SVN',
          'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
        "EU", flag) AS flag_eu
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
    -- WHERE event_start BETWEEN TIMESTAMP_ADD (first_timestamp, INTERVAL 14 DAY) AND TIMESTAMP_SUB (last_timestamp, INTERVAL 14 DAY)
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
      WHERE flags_to_same_port > 1 AND port_flag IS NOT NULL
      )
    -- WHERE domestic_reflagging > 0
  ),

  categories AS (
    SELECT * EXCEPT (flags_to_same_port, domestic_reflagging, shift_in_eu)
    FROM (
      SELECT *,
        FIRST_VALUE (flag) OVER (
          PARTITION BY vessel_record_id, port_flag
          ORDER BY first_timestamp DESC, last_timestamp DESC) AS last_flag,
      FROM countings
      -- WHERE shift_in_eu != 1
      )
      ORDER BY vessel_record_id, first_timestamp, last_timestamp
  ),

  psma_port AS (
    SELECT * EXCEPT (last_flag),
      port_flag = last_flag AS domestic_reflagging,
      port_flag IN ("ALB", "AUS", "BHS", "BGD", "BRB", "BEN", "CPV", "KHM", "CAN", "CHL", "CRI", "CUB", "CIV", "DNK", "GRL", "FRO",
        "DJI", "DMA", "ECU",
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR',
        'FJI', 'FRA', 'GAB', 'GMB', 'GHA', 'GRD', 'GIN', 'GUY', 'ISL', 'IDN', 'JPN', 'KEN', 'LBR', 'LBY', 'MDG', 'MDV', 'MRT', 'MUS', 'MNE', 'MOZ', 'MMR',
        'NAM', 'NZL', 'NIC', 'NOR', 'OMN', 'PLW', 'PAN', 'PHL', 'KOR', 'RUS', 'KNA', 'VCT', 'STP', 'SEN', 'SYC', 'SLE', 'SOM', 'ZAF', 'LKA', 'SDN',
        'THA', 'TGO', 'TON', 'TTO', 'TUR', 'GBR', 'USA', 'URY', 'VUT', 'VNM' ) AS psma
    FROM categories
  ),

  continent_added AS (
    SELECT DISTINCT
      vessel_record_id, ssvid, #shipname,
      n_shipname, n_callsign, imo, flag, flag_eu, port_flag,
      IF (port_flag IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
      "EU", port_flag) AS port_flag_eu,
      first_timestamp, last_timestamp,
      domestic_reflagging, psma,
      CASE
        WHEN b.continent = "Russia"
        THEN "Asia"
        WHEN b.iso3 = "VNM"
        THEN "Asia"
        WHEN b.iso3 = "ESH"
        THEN "Africa"
        WHEN b.iso3 = "URY"
        THEN "SA"
        WHEN b.iso3 = "VEN"
        THEN "SA"
        ELSE b.continent
      END AS continent
    FROM psma_port AS a
    LEFT JOIN gfw_research.country_codes AS b
    ON a.port_flag = b.iso3
    ORDER BY vessel_record_id, first_timestamp, last_timestamp
  ),

  add_focus_port AS (
    SELECT * EXCEPT(focus_port_flag),
      ARRAY_TO_STRING (
        udfs.dedup_array (
          ARRAY_AGG (focus_port_flag)
            OVER (PARTITION BY vessel_record_id)),
        "|") AS focus_port_flag
    FROM (
      SELECT *, IF (flag_eu = port_flag_eu, port_flag_eu, NULL) AS focus_port_flag
      FROM continent_added )
  ),

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
  ),

  attach_port_visits AS (
    SELECT DISTINCT
      * EXCEPT (port_flag),
      IF (port_flag IN (
        'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
        'CZE', 'DNK', 'EST', 'FIN', 'FRA',
        'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
        'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
        'POL', 'PRT', 'ROU', 'SVK', 'SVN',
        'ESP', 'SWE', 'GBR'), #'CYM', 'GIB', 'GRL'
      "EU", port_flag) AS port_flag_eu,
    FROM (
      SELECT vessel_record_id, ssvid, n_shipname, n_callsign, imo, flag, flag_eu, first_timestamp, last_timestamp,
        IF (focus_port_flag LIKE "%|%", port_flag_eu, focus_port_flag) AS focus_port_flag,
        domestic_reflagging, psma, continent,
        EXTRACT (YEAR FROM event_start) || "-" ||
          FORMAT ("%02d", EXTRACT (MONTH FROM event_start)) AS event_month, port_visits.port_flag
      FROM corrections #add_focus_port
      LEFT JOIN port_visits
      USING (ssvid)
      WHERE event_start BETWEEN TIMESTAMP_ADD (first_timestamp, INTERVAL 14 DAY) AND TIMESTAMP_SUB (last_timestamp, INTERVAL 14 DAY)
    )
    ORDER BY vessel_record_id, first_timestamp, event_month
  )
-- SELECT *
-- FROM combined
-- WHERE vessel_record_id = "IMO-8915823"
-- SELECT DISTINCT vessel_record_id
-- FROM attach_port_visits #countings #combined
-- WHERE vessel_record_id IN (SELECT vessel_record_id FROM in_identity_dataset WHERE flag_eu = "SEN")
-- ORDER BY vessel_record_id#, first_timestamp, last_timestamp
-- SELECT *
-- FROM in_identity_dataset
-- WHERE vessel_record_id IN (SELECT vessel_record_id FROM in_identity_dataset WHERE flag_eu = "SEN")


SELECT
  vessel_record_id, ssvid, n_shipname, n_callsign, flag_eu, first_timestamp, last_timestamp,
  focus_port_flag, domestic_reflagging, psma, continent, #port_flag_eu,
  event_month,
  LOGICAL_OR (flag_eu != port_flag_eu AND port_flag_eu IN UNNEST (flags)) AS foreign_port_of_relevance,
  LOGICAL_OR (flag_eu = port_flag_eu) AS domestic_port_of_relevance,
  -- LOGICAL_OR (port_flag_eu IN UNNEST (flags)) OVER (PARTITION BY vessel_record_id, ssvid, event_month) AS relevant_flag,
  STRING_AGG (DISTINCT port_flag_eu, "|" ORDER BY port_flag_eu) AS visited_ports_eu
FROM (
  SELECT *,
      udfs.dedup_array (ARRAY_AGG (flag_eu) OVER (PARTITION BY vessel_record_id)) AS flags
  FROM attach_port_visits ORDER BY vessel_record_id, event_month )
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY vessel_record_id, event_month


-- SELECT port_flag, psma, COUNTIF (domestic_reflagging) / COUNT (DISTINCT vessel_record_id) ratio, COUNT (DISTINCT vessel_record_id) AS cnt
-- FROM (
--   SELECT DISTINCT vessel_record_id, domestic_reflagging, psma, port_flag
--   FROM continent_added )
-- GROUP BY 1,2
-- ORDER BY psma DESC, cnt DESC, ratio DESC