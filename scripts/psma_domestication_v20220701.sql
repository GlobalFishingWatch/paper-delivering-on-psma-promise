CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2012-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2022-01-01");

-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_domestication_by_psma_v20220728` AS

WITH
  vessel_info AS (
    SELECT ssvid, activity.first_timestamp, activity.last_timestamp, best.best_flag
    FROM `gfw_research.vi_ssvid_v20220601`
    WHERE (udfs.is_fishing (best.best_vessel_class)
        OR udfs.is_carrier (best.best_vessel_class))
      -- AND TIMESTAMP_DIFF (activity.last_timestamp, activity.first_timestamp, SECOND) > 60 * 60 * 24 * 30
  ),

  port_visits AS (
    SELECT event_id, event_start, EXTRACT (YEAR FROM event_start) AS year,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag
    FROM `pipe_production_v20201001.published_events_port_visits`
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 4
  ),

  target_vessels AS (
    SELECT *
    FROM port_visits
    JOIN vessel_info
    USING (ssvid)
  ),

  group_eu_flag AS (
    SELECT *,
      IF (port_flag IN (
          'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
          'CZE', 'DNK', 'EST', 'FIN', 'FRA',
          'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
          'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
          'POL', 'PRT', 'ROU', 'SVK', 'SVN',
          'ESP', 'SWE', 'GBR'),
        "EU", port_flag) AS port_flag_eu,
      IF (flag IN (
          'AUT', 'BEL', 'BGR', 'HRV', 'CYP',
          'CZE', 'DNK', 'EST', 'FIN', 'FRA',
          'DEU', 'GRC', 'HUN', 'IRL', 'ITA',
          'LVA', 'LTU', 'LUX', 'MLT', 'NLD',
          'POL', 'PRT', 'ROU', 'SVK', 'SVN',
          'ESP', 'SWE', 'GBR'),
        "EU", flag) AS flag_eu
    FROM (
      SELECT *,
        -- udfs.mmsi_to_iso3 (ssvid) AS flag,
        best_flag AS flag
      FROM target_vessels )
  ),

  exclude_domestic_only AS (
    SELECT *,COUNTIF (flag_eu = port_flag_eu) OVER (PARTITION BY ssvid) / COUNT (*) OVER (PARTITION BY ssvid) AS ratio
    FROM (
      SELECT *,
        LOGICAL_AND (flag_eu = port_flag_eu) OVER (PARTITION BY ssvid) AS domestic_only
        -- COUNTIF (flag_eu = port_flag_eu) OVER (PARTITION BY ssvid) / COUNT (*) OVER (PARTITION BY ssvid) >= 0.9 AS domestic_only
      FROM group_eu_flag )
    WHERE NOT domestic_only
  ),

  years AS (
    SELECT year
    FROM UNNEST (GENERATE_ARRAY (2012, 2022, 1)) AS year
  ),

  foreign_vs_domestic AS (
    SELECT *, flag_eu = port_flag_eu AS domestic_flag
    FROM (
      SELECT *, #udfs.mmsi_to_iso3 (ssvid) AS flag,
      FROM exclude_domestic_only
      WHERE port_flag IS NOT NULL
        AND flag IS NOT NULL )
  ),

  psma_port AS (
    SELECT *,
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
    FROM foreign_vs_domestic
  ),

  port_by_year AS (
    SELECT
      -- IF (NOT psma AND port_flag_eu = "CHN", "CHN", CAST (psma AS STRING)) AS psma,
      psma,
      year, port_flag_eu,
      COUNTIF (domestic_flag) AS domestic_cnt, COUNT (*) AS total,
      COUNTIF (domestic_flag) / COUNT (*) AS domestic_ratio
    FROM (
      SELECT DISTINCT psma, year, ssvid, event_start, domestic_flag, port_flag_eu
      FROM psma_port
      -- WHERE port_flag_eu NOT IN ("CHN", "TWN")
       ) AS a
    JOIN years AS b
    USING (year)

    GROUP BY 1,2,3
  )


SELECT *
FROM port_by_year
WHERE year >= 2014 #AND port_flag_eu = 'RUS'
ORDER BY psma, port_flag_eu, year, domestic_ratio DESC  #port_flag_eu,

-- -- SELECT *
-- -- FROM (
-- SELECT *, #MAX (total) OVER (PARTITION BY port_flag_eu) AS mtotal
-- FROM port_by_year
-- -- ) WHERE mtotal > 3000
-- ORDER BY psma, year, domestic_ratio DESC #port_flag_eu,


-- SELECT DISTINCT psma, year, ssvid, event_start, domestic_flag
--       FROM psma_port
--       WHERE port_flag_eu = 'CHN' AND year = 2021
--       ORDER BY ssvid, event_start

-- SELECT DISTINCT ssvid, shipname, port_flag_eu, flag_eu, ratio
-- FROM exclude_domestic_only
-- ORDER BY ratio DESC, ssvid#, event_start