CREATE TEMP FUNCTION start_date () AS (TIMESTAMP "2014-01-01");
CREATE TEMP FUNCTION end_date () AS (TIMESTAMP "2022-01-01");
CREATE TEMP FUNCTION target_flag () AS ("NAM");

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_reflagging_port_visits_overtime_nam_v20220701` AS

WITH
  port_visits AS (
    SELECT event_id, event_start,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag
    FROM `pipe_production_v20201001.published_events_port_visits`
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 2
  ),

  combined AS (
    SELECT DISTINCT
      vessel_record_id, ssvid, shipname, n_shipname, n_callsign, imo,
      port_flag, flag, flag_eu,
      first_timestamp, last_timestamp
    FROM `scratch_jaeyoon.psma_reflagging_port_visits_overtime_new_part1_v20220701`
  ),

  countings AS (
    SELECT *,
    FROM (
        SELECT *,
          COUNT (DISTINCT flag) OVER (PARTITION BY vessel_record_id) AS flags_to_same_port
        FROM combined )
    WHERE flags_to_same_port > 1
  ),

  categories AS (
    SELECT * EXCEPT (flags_to_same_port)
    FROM (
      SELECT *,
        FIRST_VALUE (flag) OVER (
          PARTITION BY vessel_record_id #, port_flag
          ORDER BY first_timestamp DESC, last_timestamp DESC) AS last_flag,
        FIRST_VALUE (flag) OVER (
          PARTITION BY vessel_record_id #, port_flag
          ORDER BY first_timestamp ASC, last_timestamp ASC) AS first_flag,
      FROM countings
      )
      ORDER BY vessel_record_id, first_timestamp, last_timestamp
  ),

  psma_port AS (
    SELECT *,# EXCEPT (last_flag),
      -- port_flag = last_flag AS domestic_reflagging,
      -- port_flag = first_flag AS foreign_reflagging,
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
      -- domestic_reflagging,
      psma,
      -- foreign_reflagging,
      first_flag, last_flag,
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
      -- ARRAY_TO_STRING (
        udfs.dedup_array (
          ARRAY_AGG (focus_port_flag)
            OVER (PARTITION BY vessel_record_id)) AS focus_port_flag,
        -- "|") AS focus_port_flag
    FROM (
      SELECT *, IF (flag_eu = target_flag(), target_flag(), NULL) AS focus_port_flag
      FROM continent_added )
  ),

  corrections AS (
    SELECT *
    FROM add_focus_port
    WHERE vessel_record_id NOT IN ("AUS-859032|CCAMLR-75746|CCSBT-FV04618|IMO-9262833")
      AND ssvid NOT IN ("666050104")
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
        -- IF (focus_port_flag LIKE "%|%", port_flag_eu, focus_port_flag) AS focus_port_flag,
        focus_port_flag,
        -- IFNULL (domestic_reflagging, TRUE) AS domestic_reflagging,
        psma, continent,
        -- foreign_reflagging,
        FIRST_VALUE (flag_eu) OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp DESC) AS last_flag,
        EXTRACT (YEAR FROM event_start) || "-" ||
          FORMAT ("%02d", EXTRACT (MONTH FROM event_start)) AS event_month, port_visits.port_flag
      FROM corrections #add_focus_port
      LEFT JOIN UNNEST (focus_port_flag) AS focus_port_flag
      LEFT JOIN port_visits
      USING (ssvid)
      WHERE (event_start BETWEEN TIMESTAMP_ADD (first_timestamp, INTERVAL 14 DAY) AND TIMESTAMP_SUB (last_timestamp, INTERVAL 14 DAY))
        OR corrections.port_flag IS NULL
    )
    ORDER BY vessel_record_id, first_timestamp, event_month
  )

-- SELECT DISTINCT vessel_record_id, ssvid, n_shipname, n_callsign, flag, first_timestamp, last_timestamp, focus_port_flag,
-- FROM attach_port_visits #corrections #combined
-- WHERE vessel_record_id LIKE
-- --  "AUS-862580|CCSBT-FV06465"
-- "%CCSBT-FV06713%"
-- ORDER BY vessel_record_id, first_timestamp

SELECT DISTINCT
  vessel_record_id, ssvid, n_shipname, n_callsign, flag_eu,
  first_timestamp,
  IF (last_timestamp > '2022-01-01', '2022-01-01', last_timestamp) AS last_timestamp,
  focus_port_flag,
  domestic_reflagging, foreign_reflagging,
  event_month, port_flag_eu
FROM (
  SELECT *,
      last_flag = target_flag() AS domestic_reflagging,
      last_flag != target_flag() AS foreign_reflagging,
      udfs.dedup_array (ARRAY_AGG (flag_eu) OVER (PARTITION BY vessel_record_id)) AS flags
  FROM attach_port_visits
  WHERE focus_port_flag = target_flag() )
ORDER BY vessel_record_id, first_timestamp, event_month