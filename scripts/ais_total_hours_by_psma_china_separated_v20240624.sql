CREATE TEMP FUNCTION start_date() AS (DATE '2012-01-01');
CREATE TEMP FUNCTION end_date() AS (DATE '2022-01-01');
CREATE TEMPORARY FUNCTION psma_flag(flag STRING) AS ((
  SELECT
    flag IN (SELECT iso3 FROM `scratch_jaeyoon.psma_ratifiers_v20240318`)
));

CREATE
OR REPLACE
TABLE `world-fishing-827.scratch_jaeyoon.psma_ais_messages_total_hours_2012_2021_by_psma_china_separated_v20240624` AS

WITH
  segs_daily AS (
    SELECT seg_id, ssvid, hours, date
    FROM `world-fishing-827.pipe_ais_v3_alpha_published.segs_activity_daily`
    WHERE date BETWEEN start_date() and end_date()
  ),

  good_segs AS (
    SELECT seg_id
    FROM `pipe_ais_v3_alpha_published.segs_activity`
    WHERE good_seg
      AND NOT overlapping_and_short
  ),

  identity AS (
    SELECT ssvid, best.best_flag
    FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240501`
    WHERE on_fishing_list_best
  ),

  combined AS (
    SELECT
      date, psma_flag(best_flag) AS is_psma,
      best_flag = 'CHN'AS is_china,
      SUM (hours) AS total_hours
    FROM segs_daily
    JOIN good_segs
    USING (seg_id)
    JOIN identity
    USING (ssvid)
    GROUP BY 1,2,3
  )

SELECT *
FROM combined
ORDER BY is_psma, is_china, date