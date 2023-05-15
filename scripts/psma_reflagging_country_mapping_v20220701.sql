
CREATE TEMP FUNCTION target_flag () AS ("NZL");

WITH
  core_data AS (
    SELECT * EXCEPT (event_month), DATE (event_month || "-15") AS event_month
    FROM `scratch_jaeyoon.psma_reflagging_port_visits_overtime_nzl_v20220701`
  ),

  manual_removal AS (
    SELECT *
    FROM core_data #dedup
    WHERE
      vessel_record_id NOT IN (
        'AIS_based_Stitcher-512000089-510065000',
        'AIS_based_IMO-8131441',
        'CCSBT-FV06013|IMO-8729676',
        'CCSBT-FV05984|IMO-8834639|RUS-894679',
        'AUS-861507|CCAMLR-86929|IMO-9123219',
        'AIS_based_Stitcher-503568200-512082000',
        'AIS_based_IMO-4194304',
        'AUS-418781|AUS-860009|IMO-7901758')
      AND ssvid NOT IN ("666050104")
  ),

  ranking AS (
    SELECT
      *,
      RANK () OVER (PARTITION BY focus_port_flag ORDER BY start_mark DESC, end_mark DESC) AS rank_time,
    FROM (
      SELECT *,
        IF (domestic_reflagging,
          FIRST_VALUE (first_timestamp) OVER (
            PARTITION BY vessel_record_id
            ORDER BY flag_eu = target_flag() DESC, first_timestamp ASC),
          NULL) AS start_mark,
        IF (foreign_reflagging,
          FIRST_VALUE (last_timestamp) OVER (
            PARTITION BY vessel_record_id
            ORDER BY flag_eu = target_flag() DESC, last_timestamp DESC),
          NULL ) AS end_mark
      FROM manual_removal )
  )

SELECT *
FROM ranking
ORDER BY rank_time, vessel_record_id, event_month
