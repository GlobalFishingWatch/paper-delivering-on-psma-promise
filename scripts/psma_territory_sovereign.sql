CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2016-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2022-01-01");

WITH
  territorial AS (
    SELECT DISTINCT sovereign1_iso3, territory1_iso3
    FROM `gfw_research.eez_info`
    WHERE sovereign1_iso3 != territory1_iso3
      AND sovereign1_iso3 != 'NA'
  ),

  vessel_info AS (
    SELECT ssvid, activity.fishing_hours, activity.first_timestamp, activity.last_timestamp, best.best_flag
    FROM `gfw_research.vi_ssvid_v20220601`
    WHERE (udfs.is_fishing (best.best_vessel_class)
        OR udfs.is_carrier (best.best_vessel_class))
      AND activity.first_timestamp < end_date()
      AND activity.last_timestamp >= start_date()
  ),

  -- target_vessels AS (
  --   SELECT *
  --   FROM vessel_info
  --   WHERE best_flag IN (SELECT territory1_iso3 FROM territorial UNION ALL SELECT sovereign1_iso3 FROM territorial)
  -- ),

  target_vessels AS (
    SELECT *,
      best_flag IN (SELECT sovereign1_iso3 FROM territorial) AS terr_sover #SELECT sovereign1_iso3 FROM territorial UNION ALL
    FROM vessel_info
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

  combined AS (
    SELECT *
    FROM port_visits
    LEFT JOIN (
      SELECT ssvid, best_flag AS flag, terr_sover
      FROM  target_vessels #vessel_info
    )
    USING (ssvid)
  ),

  years AS (
    SELECT year
    FROM UNNEST (GENERATE_ARRAY (2012, 2021)) AS year
  ),

  -- terr_sove AS (
  --   SELECT *, a.flag = b.territory1_iso3 AS territory_to_sovereign
  --   FROM combined AS a
  --   JOIN territorial AS b
  --   ON ((a.flag = b.territory1_iso3 AND a.port_flag = b.sovereign1_iso3)
  --     OR (a.port_flag = b.territory1_iso3 AND a.flag = b.sovereign1_iso3))
  -- ),

  terr_sove AS (
    SELECT *,
      -- IF ()
    FROM combined
  ),

  terr_terr AS (
    SELECT *, a.flag = b.territory1_iso3 AS territory_to_territory
    FROM combined AS a
    JOIN territorial AS b
    ON ((a.flag = b.territory1_iso3 AND a.port_flag = b.territory1_iso3)
      OR (a.flag = b.sovereign1_iso3 AND a.port_flag = b.sovereign1_iso3))
  )

-- SELECT
--   IF (port_flag IN (SELECT sovereign1_iso3 FROM territorial ), port_flag, "OTHER" ) AS port,
--   -- port_flag,
--   year, COUNTIF (terr_sover) AS terr_sover, COUNT (*) AS total, COUNTIF (terr_sover) / COUNT (*) AS ratio #UNION ALL SELECT territory1_iso3 FROM territorial
-- FROM terr_sove
-- JOIN years
-- USING (year)
-- LEFT JOIN territorial
-- ON (terr_sove.port_flag = territorial.territory1_iso3)
-- -- WHERE sovereign1_iso3 IN ("FRA", "GBR")
-- WHERE flag != port_flag
-- GROUP BY 1,2 ORDER BY 1,2

SELECT IF (port_flag IN (SELECT territory1_iso3 FROM territorial ), "TERRITORY", "OTHER" ) AS port, year, COUNTIF (terr_sover) AS terr_sover, COUNT (*) AS total, COUNTIF (terr_sover) / COUNT (*) AS ratio #UNION ALL SELECT sovereign1_iso3 FROM territorial
FROM terr_sove
JOIN years
USING (year)
LEFT JOIN territorial
ON (terr_sove.port_flag = territorial.territory1_iso3)
-- WHERE flag != port_flag
GROUP BY 1,2 ORDER BY 1,2

-- SELECT flag, year, STRING_AGG (DISTINCT port_flag, ', ' ORDER BY port_flag) AS port_flags, COUNT (*) AS cnt
-- FROM terr_sove
-- WHERE  territory_to_sovereign
-- GROUP BY 1,2 ORDER BY 1,2

-- SELECT year, territory_to_sovereign, COUNT (*) AS cnt
-- FROM terr_sove
-- GROUP BY 1,2 ORDER BY 1,2

-- SELECT year, territory_to_territory, COUNT (*) AS cnt
-- FROM terr_terr
-- GROUP BY 1,2 ORDER BY 1,2

-- SELECT terr_cnt, cnt, terr_cnt / cnt AS ratio_count, terr_fishing, total_fishing, terr_fishing / total_fishing AS ratio
-- FROM (
--   SELECT COUNT (ssvid) AS cnt, SUM (fishing_hours) AS total_fishing,  #/ SUM (fishing_hours) AS ratio
--   FROM vessel_info), (SELECT COUNT (ssvid) AS terr_cnt, SUM (fishing_hours) AS terr_fishing FROM target_vessels)
