## Keywords: DWF vessel list for PSMA analysis v20240624
---------------------------
-- Analysis time frame
---------------------------
CREATE TEMPORARY FUNCTION start_date() AS (TIMESTAMP "2015-01-01");
CREATE TEMPORARY FUNCTION end_date() AS (TIMESTAMP "2022-01-01");

--------------------
-- Destination table
--------------------
CREATE OR REPLACE TABLE `world-fishing-827.scratch_jaeyoon.psma_dwf_vessel_list_v20240624` AS

WITH
  ----------------
  -- Source tables
  ----------------
  source_vessel_info AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.vi_ssvid_v20240501`
  ),

  source_ais_messages AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.messages`
  ),

  source_eez_info AS (
    SELECT *
    FROM `gfw_research.eez_info`
  ),

  -----------------------------------------------------------------------
  -- Pull all fishing and carrier vessels on AIS that are active in years of question
  -----------------------------------------------------------------------
  target_vessels AS (
    SELECT ssvid, best.best_flag, best.best_vessel_class, best.best_engine_power_kw,
      udfs.is_fishing (best.best_vessel_class) AS is_fishing,
      udfs.is_carrier (best.best_vessel_class) AS is_carrier
    FROM source_vessel_info
    WHERE
      ((udfs.is_fishing (best.best_vessel_class)
      OR (best.best_vessel_class IN ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory'))))
      -- (on_fishing_list_best
      --   OR udfs.is_carrier (best.best_vessel_class))
      AND activity.last_timestamp >= start_date()
      AND activity.first_timestamp < end_date()
  ),

  -----------------------------------------
  -- Get fishing hours of each AIS position
  -----------------------------------------
  fishing_effort AS (
    SELECT
      ssvid, timestamp, lat, lon, hours, best_vessel_class,
      regions.eez,
    FROM target_vessels
    JOIN source_ais_messages
    USING (ssvid)
    WHERE timestamp >= start_date()
      AND timestamp < end_date()
      AND clean_segs
      AND (
        (best_vessel_class != 'squid_jigger' AND nnet_score > 0.5)
        OR (best_vessel_class = 'squid_jigger' AND night_loitering > 0.5))
  ),

  ---------------------------------------------------------------
  -- Calculate fishing hours inside its domestic waters by vessel
  ---------------------------------------------------------------
  fishing_effort_outside_its_eezs AS (
    SELECT
      ssvid,
      SAFE_DIVIDE (SUM (IF (sovereign1_iso3 = best_flag, hours, 0)), SUM (hours)) AS frac_fishing_inside_eez
    FROM fishing_effort
    LEFT JOIN UNNEST (eez) AS eez
    LEFT JOIN (
      SELECT CAST (eez_id AS STRING) AS eez, sovereign1_iso3
      FROM source_eez_info
    )
    USING (eez)
    JOIN (
      SELECT DISTINCT ssvid, best_flag
      FROM target_vessels )
    USING (ssvid)
    GROUP BY 1
  ),

  -------------------------------------------------------------------------------
  -- Select only the vessels that fished inside its domestic waters less than 95%
  -------------------------------------------------------------------------------
  target_fishing_vessels AS (
    SELECT *
    FROM target_vessels
    JOIN fishing_effort_outside_its_eezs
    USING (ssvid)
    WHERE is_fishing
      AND frac_fishing_inside_eez < 0.95
  ),

  ------------------------------
  -- Include all carrier vessels
  ------------------------------
  target_carrier_vessels AS (
    SELECT *, NULL AS frac_fishing_inside_eez
    FROM target_vessels
    WHERE is_carrier
  )

--------------------------------------------------------------------------------------
-- Pull all (fishing + carrier) vessels that we would want to examine for the analysis
--------------------------------------------------------------------------------------
SELECT *
FROM target_fishing_vessels
UNION ALL
SELECT *
FROM target_carrier_vessels
