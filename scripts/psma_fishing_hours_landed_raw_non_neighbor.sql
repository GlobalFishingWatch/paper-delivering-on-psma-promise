----------------------------
-- PSMA closed loop par port
----------------------------
CREATE TEMPORARY FUNCTION start_date() AS (TIMESTAMP "2015-01-01");
CREATE TEMPORARY FUNCTION end_date() AS (TIMESTAMP "2022-07-01");
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

-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_closed_loop_by_port_byyear` AS
-- CREATE TABLE `world-fishing-827.scratch_jaeyoon.psma_fishing_inside_eez` AS
-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_country_outside_eezs_24m_summary_byyear` AS
-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_country_outside_eezs_24m_raw_byweek` AS
-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_country_outside_eezs_24m_raw_daily` AS
-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_country_outside_eezs_24m_raw_monthly_v2` AS
-- CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_country_outside_eezs_24m_raw_yearly` AS
CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_country_outside_eezs_24m_raw_monthly_v3` AS

WITH
  target_vessels AS (
    SELECT ssvid, activity.first_timestamp, activity.last_timestamp, best.best_flag, best.best_vessel_class
    FROM `gfw_research.vi_ssvid_v20220601`
    WHERE ((udfs.is_fishing (best.best_vessel_class)
        OR udfs.is_carrier (best.best_vessel_class)))
      AND (best.best_length_m > 24 OR best.best_tonnage_gt > 100)
  ),

  fishing_effort AS (
    SELECT
      ssvid, timestamp, lat, lon, hours, best_vessel_class,
      regions.eez,
    FROM target_vessels
    JOIN `gfw_research.pipe_v20201001_fishing`
    USING (ssvid)
    WHERE _PARTITIONTIME >= start_date()
      AND _PARTITIONTIME < end_date()
      AND seg_id IN (
        SELECT seg_id
        FROM `gfw_research.pipe_v20201001_segs`
        WHERE good_seg
          AND NOT overlapping_and_short )
      AND (
        (best_vessel_class != 'squid_jigger' AND nnet_score > 0.5)
        OR (best_vessel_class = 'squid_jigger' AND night_loitering > 0.5))
  ),

  fishing_effort_non_neighbors AS (
    SELECT
      ssvid,
      SAFE_DIVIDE (
        SUM (
          IF (best_flag = sovereign1_iso3
              OR EXISTS (SELECT * FROM UNNEST (eez_neighbors) AS en WHERE sovereign1_iso3 = en),
            hours, 0)), SUM (hours)) AS frac_fishing_inside_eez
    FROM fishing_effort
    LEFT JOIN UNNEST (eez) AS eez
    LEFT JOIN (
      SELECT DISTINCT CAST (eez_id AS STRING) AS eez, sovereign1_iso3
      FROM `gfw_research.eez_info` )
    USING (eez)
    JOIN (
      SELECT ssvid, best_flag
      FROM target_vessels )
    USING (ssvid)
    LEFT JOIN (
      SELECT eez, ARRAY_AGG (eez_neighbor) AS eez_neighbors
      FROM `gfw_research_precursors.eez_neighbors`
      WHERE eez IN ('CHN', 'KOR', 'JPN', 'TWN', 'VNM')
      GROUP BY 1 ) AS neighbor_info
    ON best_flag = neighbor_info.eez
    GROUP BY 1
  ),

  target_fishing_vessels AS (
    SELECT DISTINCT ssvid
    FROM target_vessels
    JOIN fishing_effort_non_neighbors
    USING (ssvid)
    WHERE frac_fishing_inside_eez < 0.9
  ),

  port_visits AS (
    SELECT event_id, event_start, event_end, EXTRACT (YEAR FROM event_start) AS year,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag,
      JSON_EXTRACT_SCALAR (event_info, "$.intermediate_anchorage.anchorage_id") AS anchorage_id,
    FROM `pipe_production_v20201001.published_events_port_visits`
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 4
  ),

------------------------------------------------
-- anchorage ids that represent the Panama Canal
------------------------------------------------
  canal_ids AS (
    SELECT s2id AS anchorage_id, label, sublabel, iso3
    FROM `anchorages.named_anchorages_v20220511`
    WHERE sublabel = "PANAMA CANAL"
      OR label = "SUEZ" OR label = "SUEZ CANAL" OR label = "SINGAPORE"
  ),

  target_vessel_port_visits AS (
    SELECT *
    FROM port_visits
    JOIN (
      SELECT ssvid, best_flag, best_vessel_class
      FROM target_vessels )
    USING (ssvid)
    WHERE anchorage_id NOT IN (SELECT anchorage_id FROM canal_ids)
  ),

  exclude_domestic_only AS (
    SELECT *
    FROM target_vessel_port_visits
    JOIN target_fishing_vessels
    USING (ssvid)
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
      a.event_start,
      a.event_end,
      IF (a.type_1 = "fishing", a.ssvid_1, a.ssvid_2) AS fishing_ssvid,
      IF (a.type_1 = "carrier", a.ssvid_1, a.ssvid_2) AS carrier_ssvid,
      IF (a.type_1 = "fishing", a.flag_1, a.flag_2) AS fishing_flag,
      IF (a.type_1 = "carrier", a.flag_1, a.flag_2) AS carrier_flag
    FROM encounters AS a
    LEFT JOIN (
      SELECT ssvid, best_flag
      FROM target_vessels ) AS b
    ON (a.ssvid_1 = b.ssvid)
    LEFT JOIN (
      SELECT ssvid, best_flag
      FROM target_vessels ) AS c
    ON (a.ssvid_2 = c.ssvid)
    WHERE b.ssvid IS NOT NULL OR c.ssvid IS NOT NULL
  ),

  target_vessel_encounters_port_added AS (
    SELECT * EXCEPT (port_visit_rank)
    FROM (
      SELECT * EXCEPT (diff), RANK () OVER (PARTITION BY fishing_ssvid, event_start ORDER BY diff ASC) AS port_visit_rank
      FROM (
        SELECT a.*, b.port_flag, TIMESTAMP_DIFF (b.event_start, a.event_end, SECOND) AS diff
        FROM target_vessel_encounters AS a
        LEFT JOIN (
          SELECT ssvid, port_flag, event_start, event_end
          FROM target_vessel_port_visits ) AS b
        ON a.carrier_ssvid = b.ssvid
        WHERE b.event_start > a.event_end ) )
    WHERE port_visit_rank = 1
  ),

  target_vessel_landings AS (
    SELECT *
    FROM (
      SELECT DISTINCT
        ssvid, event_start, event_end, "port_visit" AS landing_type, port_flag,
        best_flag AS flag, udfs.is_fishing (best_vessel_class) AS is_fishing
      FROM  exclude_domestic_only #target_vessel_port_visits
      UNION DISTINCT
      SELECT DISTINCT
        fishing_ssvid AS ssvid, event_start, event_end, "transshipment" AS landing_type, port_flag,
        -- fishing_flag AS flag,
        carrier_flag AS flag,
        TRUE AS is_fishing
      FROM target_vessel_encounters_port_added )
  ),

  fishing_events AS (
    SELECT
      ssvid,
      timestamp,
      IF ((best_vessel_class = "squid_jigger" AND night_loitering > 0.5)
          OR (best_vessel_class != "squid_jigger" AND nnet_score >0.5),
        hours, 0) AS fishing_hours
    FROM `gfw_research.pipe_v20201001_fishing`
    LEFT JOIN UNNEST (regions.eez) AS eez
    LEFT JOIN (
      SELECT ssvid, best_vessel_class, best_flag, group_eu_flags (best_flag) AS best_flag_eu
      FROM target_vessels )
    USING (ssvid)
    LEFT JOIN (
      SELECT DISTINCT CAST (eez_id AS STRING) AS eez, sovereign1_iso3, group_eu_flags (sovereign1_iso3) AS sovereign1_iso3_eu
      FROM `gfw_research.eez_info` )
    USING (eez)
    LEFT JOIN (
      SELECT eez, ARRAY_AGG (eez_neighbor) AS eez_neighbors
      FROM `gfw_research_precursors.eez_neighbors`
      WHERE eez IN ('CHN', 'KOR', 'JPN', 'VNM', 'TWN')
      GROUP BY 1 ) AS neighbor_info
    ON best_flag = neighbor_info.eez
    WHERE timestamp BETWEEN start_date() AND end_date()
      AND (sovereign1_iso3 IS NULL
        OR (sovereign1_iso3 != best_flag
          AND NOT EXISTS (SELECT * FROM UNNEST (eez_neighbors) AS en WHERE sovereign1_iso3 = en)))
  ),

  monthly AS (
    SELECT time_interval, LEAD (time_interval) OVER (ORDER BY time_interval) AS next_time_interval
    FROM UNNEST (GENERATE_DATE_ARRAY(DATE (start_date()), DATE (end_date()), INTERVAL 1 MONTH)) AS time_interval
  ),

  combined AS (
    SELECT
      psma_flag (port_flag) AS psma,
      group_eu_flags (port_flag) AS port_flag,
      time_interval AS timeline,
      group_eu_flags(flag) = group_eu_flags(port_flag) AS domestic,
      IFNULL (SUM (fishing_hours), 0) AS total_fishing_hours,
    FROM fishing_events AS a
    JOIN (
      SELECT *,
        IFNULL (
          LAG (event_end) OVER (PARTITION BY ssvid ORDER BY event_end),
          TIMESTAMP_SUB (event_end, INTERVAL 365 DAY)) AS prev_event_end
      FROM target_vessel_landings ) AS b
    ON a.ssvid = b.ssvid
      AND a.timestamp BETWEEN b.prev_event_end AND b.event_start
    LEFT JOIN monthly AS c
    ON DATE (a.timestamp) >= c.time_interval
      AND DATE (a.timestamp) < c.next_time_interval
    WHERE flag IS NOT NULL AND port_flag IS NOT NULL
      AND a.timestamp > TIMESTAMP_SUB (b.event_start, INTERVAL 365 DAY)
    GROUP BY 1,2,3,4
  ),

  sumup AS (
    SELECT
      psma, port_flag,
      timeline,
      SUM (IF (domestic, total_fishing_hours, 0)) AS domestic_fishing_hours_landed,
      SUM (total_fishing_hours) AS total_fishing_hours_landed,
      SAFE_DIVIDE (SUM (IF (domestic, total_fishing_hours, 0)), SUM (total_fishing_hours)) AS frac,
      -- MAX (SUM (total_fishing_hours)) OVER (PARTITION BY psma, port_flag) AS max_total,
      SUM (SUM (total_fishing_hours)) OVER (PARTITION BY psma, port_flag) AS sum_total,
    FROM (
      SELECT *
      FROM combined
      UNION ALL
      SELECT psma, "ALL" AS port_flag, timeline, domestic, total_fishing_hours
      FROM combined )
    GROUP BY 1,2,3
  ),

  table_format AS (
    SELECT psma, port_flag, time_interval AS timeline
    FROM monthly
    LEFT JOIN (
      SELECT DISTINCT psma, port_flag
      FROM sumup )
    ON TRUE
  )

-- SELECT *
-- FROM sumup
-- WHERE port_flag = "KOR"
-- ORDER BY timeline

SELECT psma, port_flag, timeline,
  IFNULL (domestic_fishing_hours_landed, 0) AS domestic_fishing_hours_landed,
  IFNULL (total_fishing_hours_landed, 0) AS total_fishing_hours_landed,
  IFNULL (frac, NULL) AS frac,
  IFNULL (sum_total, 0) AS sum_total
FROM table_format
LEFT JOIN sumup
USING (psma, port_flag, timeline)
ORDER BY psma, port_flag, timeline
