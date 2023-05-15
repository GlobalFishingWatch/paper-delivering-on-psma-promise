------------------------------
-- PSMA closed loop by carrier
------------------------------
CREATE TEMPORARY FUNCTION start_date() AS (TIMESTAMP "2016-01-01");
CREATE TEMPORARY FUNCTION end_date() AS (TIMESTAMP "2021-12-31");
CREATE TEMPORARY FUNCTION psma_flag(flag STRING) AS ((
  SELECT
    flag IN (
      "EU",
      "ALB", "AUS", "BHS", "BGD", "BRB", "BEN", "CPV", "KHM", "CAN", "CHL", "CRI", "CUB", "CIV", "DNK", "GRL", "FRO",
      "DJI", "DMA", "ECU",
      'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
      'CZE', 'DNK', 'EST', 'FIN', 'FRA',
      'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
      'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
      'POL', 'PRT', 'ROU', 'SVK', 'SVN',
      'ESP', 'SWE', 'GBR',
      'FJI', 'FRA', 'GAB', 'GMB', 'GHA', 'GRD', 'GIN', 'GUY', 'ISL', 'IDN', 'JPN', 'KEN', 'LBR', 'LBY', 'MDG', 'MDV', 'MRT', 'MUS', 'MNE', 'MOZ', 'MMR',
      'NAM', 'NZL', 'NIC', 'NOR', 'OMN', 'PLW', 'PAN', 'PHL', 'KOR', 'RUS', 'KNA', 'VCT', 'STP', 'SEN', 'SYC', 'SLE', 'SOM', 'ZAF', 'LKA', 'SDN',
      'THA', 'TGO', 'TON', 'TTO', 'TUR', 'GBR', 'USA', 'URY', 'VUT', 'VNM' )
));
CREATE TEMPORARY FUNCTION group_eu_flags (flag STRING) AS ((
  SELECT
    CASE
      WHEN
        flag IN (
          'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
          'CZE', 'DNK', 'EST', 'FIN', 'FRA',
          'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
          'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
          'POL', 'PRT', 'ROU', 'SVK', 'SVN',
          'ESP', 'SWE', 'GBR')
      THEN "EU"
      ELSE flag
    END
));

-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_closed_loop_by_carrier` AS

WITH

  target_vessels AS (
    SELECT ssvid, activity.first_timestamp, activity.last_timestamp, best.best_flag, best.best_vessel_class
    FROM `gfw_research.vi_ssvid_v20220401`
    WHERE TIMESTAMP_DIFF (activity.last_timestamp, activity.first_timestamp, SECOND) > 60 * 60 * 24 * 30
      -- AND (udfs.is_fishing (best.best_vessel_class)
      --   OR udfs.is_carrier (best.best_vessel_class))
  ),

  port_visits AS (
    SELECT event_id, event_start, event_end, EXTRACT (YEAR FROM event_start) AS year,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag,
      JSON_EXTRACT_SCALAR (event_info, "$.intermediate_anchorage.anchorage_id") AS anchorage_id,
    FROM `pipe_production_v20201001.published_events_port_visits`
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 2
  ),

------------------------------------------------
-- anchorage ids that represent the Panama Canal
------------------------------------------------
  canal_ids AS (
    SELECT s2id AS anchorage_id, label, sublabel, iso3
    FROM `anchorages.named_anchorages_v20220511`
    WHERE sublabel = "PANAMA CANAL"
      OR label = "SUEZ" OR label = "SUEZ CANAL"
  ),

  target_vessel_port_visits AS (
    SELECT *
    FROM port_visits
    -- JOIN (
    --   SELECT ssvid, best_flag
    --   FROM target_vessels )
    -- USING (ssvid)
    WHERE anchorage_id NOT IN (SELECT anchorage_id FROM canal_ids)
  ),

  encounters AS (
    SELECT DISTINCT
      event_id, event_start, event_end, lat_mean, lon_mean,
      JSON_EXTRACT_SCALAR (event_info, "$.median_distance_km") AS median_distance_km,
      JSON_EXTRACT_SCALAR (event_info, "$.median_speed_knots") AS median_speed_knots,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS name_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].type") AS type_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].flag") AS flag_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].ssvid") AS ssvid_2,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].name") AS name_2,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].type") AS type_2,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].flag") AS flag_2
    FROM `world-fishing-827.pipe_production_v20201001.published_events_encounters`
    WHERE event_start BETWEEN start_date() AND end_date()
      AND JSON_EXTRACT_SCALAR (event_info, "$.vessel_classes") IN ("carrier-fishing", "fishing-carrier")
  ),

  target_vessel_encounters AS (
    SELECT DISTINCT
      a.event_id,
      a.event_start,
      a.event_end,
      IF (a.type_1 = "fishing", a.ssvid_1, a.ssvid_2) AS fishing_ssvid,
      IF (a.type_1 = "carrier", a.ssvid_1, a.ssvid_2) AS carrier_ssvid,
      IF (a.type_1 = "fishing", a.flag_1, a.flag_2) AS fishing_flag,
      IF (a.type_1 = "carrier", a.flag_1, a.flag_2) AS carrier_flag
    FROM encounters AS a
    -- LEFT JOIN (
    --   SELECT ssvid, best_flag
    --   FROM target_vessels ) AS b
    -- ON (a.ssvid_1 = b.ssvid)
    -- LEFT JOIN (
    --   SELECT ssvid, best_flag
    --   FROM target_vessels ) AS c
    -- ON (a.ssvid_2 = c.ssvid)
  ),

  target_vessel_port_rank AS (
    SELECT
      EXTRACT (YEAR FROM port_start) AS year,
      *,
      psma_flag (carrier_flag) AS psma_carrier,
      psma_flag (port_flag) AS psma_port,
      RANK() OVER (PARTITION BY carrier_ssvid, encounter_end ORDER BY diff ASC) AS post_visit_rank
    FROM (
      SELECT DISTINCT
        a.event_end AS encounter_end,
        a.fishing_ssvid,
        a.fishing_flag,
        group_eu_flags (a.fishing_flag) AS fishing_flag_eu,
        a.carrier_ssvid,
        a.carrier_flag,
        group_eu_flags (a.carrier_flag) AS carrier_flag_eu,
        port_flag,
        group_eu_flags (port_flag) AS port_flag_eu,
        b.event_start AS port_start,
        TIMESTAMP_DIFF (b.event_start, a.event_end, SECOND) AS diff
      FROM target_vessel_encounters AS a
      LEFT JOIN (
        SELECT event_start, ssvid, port_flag
        FROM target_vessel_port_visits ) AS b
      ON (a.carrier_ssvid = b.ssvid)
      -- ON (a.fishing_ssvid = b.ssvid)
      WHERE b.event_start > a.event_end )
  ),

  target_vessel_first_port AS (
    SELECT DISTINCT * EXCEPT (post_visit_rank, diff)
    FROM target_vessel_port_rank
    WHERE post_visit_rank = 1
  ),

  target_vessel_second_port AS (
    SELECT encounter_end, fishing_ssvid, carrier_ssvid, port_flag_eu, port_start
    FROM (
      SELECT
        *,
        RANK() OVER (PARTITION BY carrier_ssvid, encounter_end ORDER BY diff ASC) AS post_visit_rank_for_second
      FROM target_vessel_port_rank
      WHERE post_visit_rank > 1
        AND port_flag_eu != "RUS" )
    WHERE post_visit_rank_for_second = 1
  ),

  psma_flag AS (
    SELECT *, #psma_flag (flag) AS psma
    FROM encounters
  ),

  fishing_events AS (
    SELECT
      ssvid,
      timestamp AS event_start,
      IF (night_loitering > 0 OR nnet_score >0, hours, 0) AS hours
    FROM `gfw_research.pipe_v20201001_fishing`
    WHERE timestamp BETWEEN start_date() AND end_date()
  )

SELECT second_port_flag_eu, COUNT (*) AS cnt, ROUND (AVG (diff_in_day), 0) AS gap_in_day
FROM (
  SELECT a.*, b.port_flag_eu AS second_port_flag_eu, b.port_start AS second_port_start,
    TIMESTAMP_DIFF (b.port_start, a.port_start, SECOND) / 60 / 60 / 24 AS diff_in_day
  FROM target_vessel_first_port AS a
  LEFT JOIN target_vessel_second_port AS b
  USING (encounter_end, fishing_ssvid, carrier_ssvid)
  WHERE a.carrier_flag_eu = 'RUS' )
GROUP BY 1 ORDER BY cnt DESC

-- SELECT psma_flag (carrier_flag_eu) AS psma, carrier_flag_eu, closed_loop_cnt, total_cnt, closed_loop_cnt / total_cnt AS ratio_closed_loop
-- FROM (
--   SELECT carrier_flag_eu,
--     COUNTIF (fishing_flag_eu = carrier_flag_eu AND carrier_flag_eu = port_flag_eu) AS closed_loop_cnt,
--     COUNT (*) AS total_cnt
--   FROM target_vessel_first_port
--   GROUP BY 1)
-- -- WHERE port_flag IN ('TWN', 'CHN', 'RUS', 'KOR', 'USA', 'JPN', 'IDN', 'TUR', 'CHL', 'ESP')
-- WHERE closed_loop_cnt / total_cnt > 0.0 AND total_cnt > 10
-- ORDER BY psma, carrier_flag_eu, total_cnt
