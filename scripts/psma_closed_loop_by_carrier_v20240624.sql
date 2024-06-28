------------------------------
-- PSMA closed loop by carrier
------------------------------
CREATE TEMPORARY FUNCTION start_date() AS (TIMESTAMP "2015-01-01");
CREATE TEMPORARY FUNCTION end_date() AS (TIMESTAMP "2022-01-01");
CREATE TEMPORARY FUNCTION psma_flag(flag STRING) AS ((
  SELECT
    flag IN (SELECT iso3 FROM `scratch_jaeyoon.psma_ratifiers_v20240318`)
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

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_closed_loop_by_carrier_v20240624` AS

WITH
  source_vessel_info AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240501`
  ),

  source_port_visits AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.product_events_port_visit`
  ),

  source_anchorages AS (
    SELECT *
    FROM `world-fishing-827.anchorages.named_anchorages_v20240117`
  ),

  source_encounters AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.product_events_encounter`
  ),

  port_visits AS (
    SELECT event_id, event_start, event_end, EXTRACT (YEAR FROM event_start) AS year,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag,
      JSON_EXTRACT_SCALAR (event_info, "$.intermediate_anchorage.anchorage_id") AS anchorage_id,
    FROM source_port_visits
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 3
      AND TIMESTAMP_DIFF (event_end, event_start, HOUR) > 3
  ),

------------------------------------------------
-- anchorage ids that represent the Panama Canal
------------------------------------------------
  canal_ids AS (
    SELECT s2id AS anchorage_id, label, sublabel, iso3
    FROM source_anchorages
    WHERE sublabel = "PANAMA CANAL"
      OR label = "SUEZ"
      OR label = "SUEZ CANAL"
      OR label = "SINGAPORE"
  ),

  target_carriers AS (
    SELECT *
    FROM source_vessel_info
    WHERE best.best_vessel_class IN ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory')
  ),

  -----------------------------------------------
  -- Remove port visits in unlikely landing ports
  -----------------------------------------------
  target_vessel_port_visits AS (
    SELECT *
    FROM port_visits
    WHERE anchorage_id NOT IN (SELECT anchorage_id FROM canal_ids)
  ),

  -------------------
  -- Encounter events
  -------------------
  encounters AS (
    SELECT DISTINCT
      event_id, event_start, event_end, lat_mean, lon_mean,
      CAST (JSON_EXTRACT_SCALAR (event_info, "$.median_distance_km") AS FLOAT64) AS median_distance_km,
      CAST (JSON_EXTRACT_SCALAR (event_info, "$.median_speed_knots") AS FLOAT64) AS median_speed_knots,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS name_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].type") AS type_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].flag") AS flag_1,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].ssvid") AS ssvid_2,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].name") AS name_2,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].type") AS type_2,
      JSON_EXTRACT_SCALAR (event_vessels, "$[1].flag") AS flag_2
    FROM source_encounters
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
    WHERE median_distance_km < 0.5
      AND median_speed_knots < 2
      AND TIMESTAMP_DIFF (event_end, event_start, HOUR) > 2
  ),

  -----------------------------------------------------------
  -- Identify the closest port visits after a given encounter
  -----------------------------------------------------------
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
      JOIN (
        SELECT DISTINCT ssvid
        FROM target_carriers ) AS c
      ON a.carrier_ssvid = c.ssvid
      WHERE b.event_start > a.event_end )
  ),

  target_vessel_first_port AS (
    SELECT DISTINCT * EXCEPT (post_visit_rank, diff)
    FROM target_vessel_port_rank
    WHERE post_visit_rank = 1
  )

SELECT psma_flag (carrier_flag_eu) AS psma, carrier_flag_eu, closed_loop_cnt, total_cnt, closed_loop_cnt / total_cnt AS ratio_closed_loop,
  grand_total_cnt,
  total_cnt / grand_total_cnt AS grand_ratio,
  closed_loop_cnt / closed_loop_total_cnt AS closed_loop_ratio,
  closed_loop_total_cnt / grand_total_cnt AS closed_ratio
FROM (
  SELECT carrier_flag_eu,
    COUNTIF (fishing_flag_eu = carrier_flag_eu AND carrier_flag_eu = port_flag_eu) AS closed_loop_cnt,
    COUNT (*) AS total_cnt
  FROM target_vessel_first_port
  GROUP BY 1),
( SELECT COUNT (*) grand_total_cnt,
    COUNTIF (fishing_flag_eu = carrier_flag_eu AND carrier_flag_eu = port_flag_eu) AS closed_loop_total_cnt
  FROM target_vessel_first_port)
WHERE closed_loop_cnt / total_cnt >= 0.0 AND total_cnt > 100
ORDER BY psma, carrier_flag_eu, total_cnt

#
# Russian port visits by year
--SELECT year, COUNTIF (udfs.mmsi_to_iso3 (ssvid) = port_flag) / COUNT (*) AS ratio
--FROM port_visits
--WHERE udfs.mmsi_to_iso3 (ssvid) = 'RUS'
--GROUP BY 1
--ORDER BY 1
