CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2012-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2022-06-01");
CREATE TEMP FUNCTION target_flag() AS ("NZL");

CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_reflagging_nzl_new` AS

WITH
  vessel_info AS (
    SELECT ssvid, activity.first_timestamp, activity.last_timestamp, best.best_flag
    FROM `gfw_research.vi_ssvid_v20220501`
    WHERE TIMESTAMP_DIFF (activity.last_timestamp, activity.first_timestamp, SECOND) > 60 * 60 * 24 * 30
      AND (udfs.is_fishing (best.best_vessel_class)
        OR udfs.is_carrier (best.best_vessel_class))
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

  foreign_vs_domestic AS (
    SELECT *, target_flag() = port_flag AS domestic_flag
    FROM (
      SELECT *, udfs.mmsi_to_iso3 (ssvid) AS flag
      FROM port_visits )
    WHERE port_flag IS NOT NULL
  ),

  domestic_end AS (
    SELECT DISTINCT ssvid, last_domestic_port_visit, cnt_domestic_port_visit
    FROM (
      SELECT *,
        MAX (event_start) OVER (PARTITION BY ssvid) AS last_domestic_port_visit,
        COUNT (event_start) OVER (PARTITION BY ssvid) AS cnt_domestic_port_visit
      FROM foreign_vs_domestic
      WHERE domestic_flag )
    WHERE event_start = last_domestic_port_visit
  ),

  foreign_end AS (
    SELECT DISTINCT ssvid, last_foreign_port_visit, cnt_foreign_port_visit, first_foreign_port_visit, #foreign_ports_visited
    FROM (
      SELECT *,
        MAX (event_start) OVER (PARTITION BY ssvid) AS last_foreign_port_visit,
        COUNT (event_start) OVER (PARTITION BY ssvid) AS cnt_foreign_port_visit,
        MIN (event_start) OVER (PARTITION BY ssvid) AS first_foreign_port_visit,
        STRING_AGG (port_flag) OVER (PARTITION BY ssvid) AS foreign_ports_visited,
        -- COUNTIF (flag = port_flag)
      FROM foreign_vs_domestic
      WHERE NOT domestic_flag )
    WHERE event_start = last_foreign_port_visit
  ),

  reflagging AS (
    SELECT DISTINCT ssvid, focus_port_flag, flag_eu, vessel_record_id, first_timestamp, last_timestamp
    FROM (
      SELECT ssvid, SPLIT (focus_port_flag, "|") AS focus_port_flag, flag_eu, vessel_record_id, first_timestamp, last_timestamp
      FROM `world-fishing-827.scratch_jaeyoon.psma_reflagging_port_visits_overtime_new_v20220701` )
    LEFT JOIN UNNEST (focus_port_flag) AS focus_port_flag
  ),

  domestic_flag_visits AS (
    SELECT DISTINCT
      a.ssvid, flag,
      last_domestic_port_visit AS marker, cnt_domestic_port_visit, last_foreign_port_visit, cnt_foreign_port_visit, #foreign_ports_visited,
      d.first_timestamp, d.last_timestamp
    FROM (
      SELECT DISTINCT ssvid, flag,# port_flag
      FROM foreign_vs_domestic ) AS a
    LEFT JOIN domestic_end AS b
    USING (ssvid)
    LEFT JOIN foreign_end AS c
    USING (ssvid)
    JOIN vessel_info AS d
    USING (ssvid)
    LEFT JOIN reflagging AS e
    ON (a.ssvid = e.ssvid AND e.focus_port_flag LIKE "%" || a.flag || "%")
    WHERE
    --  last_domestic_port_visit < TIMESTAMP_SUB (end_date(), INTERVAL 90 DAY)
    --   AND (cnt_domestic_port_visit >= 5 OR last_timestamp < TIMESTAMP_SUB (end_date(), INTERVAL 365 DAY))
      -- AND
      focus_port_flag IS NULL
      AND flag = target_flag()
    -- ORDER BY last_domestic_port_visit, last_foreign_port_visit
  ),

  foreign_flag_visits AS (
    SELECT DISTINCT
      a.ssvid, flag,
      first_foreign_port_visit AS marker, cnt_domestic_port_visit, last_foreign_port_visit, cnt_foreign_port_visit,
      d.first_timestamp, d.last_timestamp
    FROM (
      SELECT DISTINCT ssvid, flag, target_flag() AS target_flag# port_flag
      FROM foreign_vs_domestic
      WHERE port_flag = target_flag() ) AS a
    LEFT JOIN domestic_end AS b
    USING (ssvid)
    LEFT JOIN foreign_end AS c
    USING (ssvid)
    JOIN vessel_info AS d
    USING (ssvid)
    LEFT JOIN reflagging AS e
    ON (a.ssvid = e.ssvid AND e.focus_port_flag LIKE "%" || target_flag || "%")
    WHERE first_foreign_port_visit > "2014-01-01"
      -- AND (cnt_domestic_port_visit >= 5 OR last_timestamp < TIMESTAMP_SUB (end_date(), INTERVAL 365 DAY))
      -- AND
        AND focus_port_flag IS NULL
        AND flag != target_flag()
    -- ORDER BY first_foreign_port_visit, last_foreign_port_visit
  ),

  corrections AS (
    SELECT * EXCEPT (first_timestamp),
      CASE
        WHEN ssvid = "440542000"
        THEN TIMESTAMP "2018-08-15 21:45:32+00:00"
        WHEN ssvid = "440450000"
        THEN TIMESTAMP "2018-08-17 01:40:23+00:00"
        ELSE first_timestamp
      END AS first_timestamp
    FROM (
      SELECT *, "domestic" AS cat,
      FROM domestic_flag_visits
      UNION ALL
      SELECT *, "foreign" AS cat,
      FROM foreign_flag_visits )

  )
-- SELECT DISTINCT
--       a.ssvid, flag, focus_port_flag
--     FROM (
--       SELECT DISTINCT ssvid, flag, target_flag() AS target_flag# port_flag
--       FROM foreign_vs_domestic
--       WHERE port_flag = target_flag() ) AS a
--     LEFT JOIN domestic_end AS b
--     USING (ssvid)
--     LEFT JOIN foreign_end AS c
--     USING (ssvid)
--     JOIN vessel_info AS d
--     USING (ssvid)
--     LEFT JOIN reflagging AS e
--     ON (a.ssvid = e.ssvid AND e.focus_port_flag LIKE "%" || target_flag || "%")
--     WHERE flag = target_flag
--     ORDER BY ssvid

-- SELECT *
-- FROM foreign_flag_visits #reflagging
-- -- WHERE flag_eu = 'SEN'
-- ORDER BY ssvid

-- SELECT vessel_record_id, ssvid, focus_port_flag, flag_eu, first_timestamp, last_timestamp
-- FROM reflagging
-- -- WHERE focus_port_flag = "SEN"
-- WHERE vessel_record_id IN (SELECT vessel_record_id FROM reflagging WHERE flag_eu = "SEN")
--   AND vessel_record_id != "AIS_based_IMO-4194304"
-- ORDER BY vessel_record_id, first_timestamp, last_timestamp, ssvid

SELECT DISTINCT
  cat, ssvid, flag, first_timestamp, last_timestamp,
  marker, last_foreign_port_visit,
  port_flag,
  EXTRACT (YEAR FROM event_start) || "-" ||
    FORMAT ("%02d", EXTRACT (MONTH FROM event_start)) AS event_month,
  target_flag() AS target_flag,
  cnt_domestic_port_visit > cnt_foreign_port_visit AS target_port_major
FROM corrections #summary
LEFT JOIN port_visits
USING (ssvid)
-- ORDER BY cat, marker DESC, last_foreign_port_visit DESC
-- WHERE ssvid =  "572405220" #"553595359" #"553111777" #