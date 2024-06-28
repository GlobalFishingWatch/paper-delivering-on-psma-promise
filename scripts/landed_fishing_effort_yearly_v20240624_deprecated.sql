## Keywords: Landed fishing effort in kwh by PSMA vs. non-PSMA fleet in PSMA vs. non-PSMA port
CREATE TEMPORARY FUNCTION start_date() AS (TIMESTAMP "2015-01-01");
CREATE TEMPORARY FUNCTION end_date() AS (TIMESTAMP "2021-11-01");
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


--------------------
-- Destination table
--------------------
CREATE
OR REPLACE
TABLE `world-fishing-827.scratch_jaeyoon.landed_fishing_effort_yearly_v20240624` AS

WITH
  ----------------
  -- Source tables
  ----------------
  source_port_visits AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.product_events_port_visit`
  ),

  source_encounters AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.product_events_encounter`
  ),

  source_anchorages AS (
    SELECT *
    FROM `world-fishing-827.anchorages.named_anchorages_v20240117`
  ),

  source_ais_messages AS (
    SELECT *
    FROM `world-fishing-827.pipe_ais_v3_published.messages`
  ),

  source_eez_info AS (
    SELECT *
    FROM `world-fishing-827.gfw_research.eez_info`
  ),

  source_ratify_info AS (
    SELECT *
    FROM `world-fishing-827.scratch_jaeyoon.psma_ratifiers_v20240318`
  ),

  target_vessels AS (
    SELECT *
    FROM `world-fishing-827.scratch_jaeyoon.psma_dwf_vessel_list_v20240624`
  ),

  ------------------
  -- Port visit data
  ------------------
  port_visits AS (
    SELECT event_id, event_start, event_end, EXTRACT (YEAR FROM event_start) AS year,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
      JSON_EXTRACT_SCALAR (event_vessels, "$[0].name") AS shipname,
      JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.flag") AS port_flag,
      JSON_EXTRACT_SCALAR (event_info, "$.intermediate_anchorage.anchorage_id") AS anchorage_id,
    FROM source_port_visits
    WHERE event_start BETWEEN start_date() AND end_date()
      AND CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) >= 3
  ),

  ----------------------------------------------------------
  -- anchorage ids that represent the unlikely landing ports
  ----------------------------------------------------------
  canal_ids AS (
    SELECT s2id AS anchorage_id, label, sublabel, iso3
    FROM source_anchorages
    WHERE sublabel = "PANAMA CANAL"
      OR label = "SUEZ"
      OR label = "SUEZ CANAL"
      OR label = "SINGAPORE"
  ),

  --------------------------------
  -- Port visits by target vessels
  --------------------------------
  target_vessel_port_visits AS (
    SELECT *
    FROM port_visits
    JOIN (
      SELECT DISTINCT ssvid, best_flag, best_vessel_class
      FROM target_vessels )
    USING (ssvid)
    WHERE anchorage_id NOT IN (SELECT anchorage_id FROM canal_ids)
  ),

  ---------------------------
  -- Get only fishing vessels
  ---------------------------
  include_only_fishing_vessels AS (
    SELECT *
    FROM target_vessel_port_visits
    JOIN target_vessels
    USING (ssvid, best_flag, best_vessel_class)
    WHERE is_fishing
  ),

  -------------------------------------------------------------------
  -- Pull vessel encounter events between fishing and carrier vessels
  -------------------------------------------------------------------
  encounters AS (
    SELECT *
    FROM (
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
        AND JSON_EXTRACT_SCALAR (event_info, "$.vessel_classes") IN ("carrier-fishing", "fishing-carrier") )
    WHERE median_distance_km < 0.5
      AND median_speed_knots < 2
      AND TIMESTAMP_DIFF (event_end, event_start, MINUTE) >  60 * 2
  ),

  ------------------------------------------------------------------
  -- Extract only the encounter events related to the target vessels
  ------------------------------------------------------------------
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
      SELECT DISTINCT ssvid, best_flag
      FROM target_vessels ) AS b
    ON (a.ssvid_1 = b.ssvid)
    LEFT JOIN (
      SELECT DISTINCT ssvid, best_flag
      FROM target_vessels ) AS c
    ON (a.ssvid_2 = c.ssvid)
    WHERE b.ssvid IS NOT NULL OR c.ssvid IS NOT NULL
  ),

  ---------------------------------------------------------------------------------
  -- Get the first port visit of carriers after the encounter with a fishing vessel
  ---------------------------------------------------------------------------------
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

  ---------------------------------------------------------------------------------------------------------
  -- Combine fishing vessel port visits and carrier vessels port visits (first port visit after encounters)
  ---------------------------------------------------------------------------------------------------------
  target_vessel_landings AS (
    SELECT *
    FROM (
      SELECT DISTINCT
        ssvid, event_start, event_end, "port_visit" AS landing_type, port_flag,
        best_flag AS flag, udfs.is_fishing (best_vessel_class) AS is_fishing
      FROM include_only_fishing_vessels # exclude_domestic_only
      UNION DISTINCT
      SELECT DISTINCT
        fishing_ssvid AS ssvid, event_start, event_end, "transshipment" AS landing_type, port_flag,
        carrier_flag AS flag,
        TRUE AS is_fishing
      FROM target_vessel_encounters_port_added
    )
  ),

  ------------------------------------------------------------------------------------------------------
  -- Fishing effort (hours * vessel engine power) outside domestic waters with regard to the vessel flag
  ------------------------------------------------------------------------------------------------------
  fishing_events AS (
    SELECT
      ssvid,
      timestamp,
      IF ((best_vessel_class = "squid_jigger" AND night_loitering > 0.5)
          OR (best_vessel_class != "squid_jigger" AND nnet_score >0.5),
        hours / repeats * best_engine_power_kw, 0) AS fishing_hours
    FROM (
      SELECT
        ssvid, timestamp, hours, nnet_score, night_loitering,
        regions.eez,
        GREATEST (1, IFNULL (ARRAY_LENGTH (regions.eez), 1)) AS repeats
      FROM source_ais_messages
      WHERE timestamp BETWEEN start_date() AND end_date()
        AND clean_segs
        AND (nnet_score > 0.5 OR night_loitering > 0.5) )
    LEFT JOIN UNNEST (eez) AS eez
    LEFT JOIN (
      SELECT ssvid, best_vessel_class, best_flag, best_engine_power_kw
      FROM target_vessels )
    USING (ssvid)
    LEFT JOIN (
      SELECT DISTINCT CAST (eez_id AS STRING) AS eez, sovereign1_iso3
      FROM source_eez_info )
    USING (eez)
    # Fishing outside domestic waters with regard to the vessel flag
    WHERE sovereign1_iso3 IS NULL OR sovereign1_iso3 != best_flag
  ),

  ------------------------------------
  -- PSMA ratification date by country
  ------------------------------------
  ratify_info AS (
    SELECT DISTINCT iso3 AS ratify_flag, TIMESTAMP (date) AS ratify_date
    FROM source_ratify_info
  ),

  -------------------------------------------------------------
  -- Combine all information to calculate landed fishing effort
  -------------------------------------------------------------
  combined AS (
    SELECT
      psma,
      port_flag = flag AS dom_landing,
      port_flag,
      timeline,
      psma = psma_port AS PSMA_group_visit,
      IFNULL (SUM (fishing_hours), 0) AS total_fishing_hours,
      COUNT (DISTINCT event_start) AS landing_count,
    FROM (
      SELECT
        psma_flag (flag) AND IFNULL (timestamp > c.ratify_date, FALSE) AS psma,
        group_eu_flags (port_flag) AS port_flag,
        group_eu_flags (flag) AS flag,
        EXTRACT (YEAR FROM a.timestamp) AS timeline,
        #
        # PSMA group port visit (PSMA vessels visiting PSMA ports OR non-PSMA vessels visiting non-PSMA ports)
        -- (NOT psma_flag (port_flag) AND NOT psma_flag (flag))
        --   OR (NOT psma_flag (port_flag) AND psma_flag (flag) AND timestamp < c.ratify_date)
        --   OR (psma_flag (port_flag) AND timestamp < d.ratify_date AND NOT psma_flag (flag))
        --   OR (psma_flag (port_flag) AND timestamp < d.ratify_date AND psma_flag (flag) AND timestamp < c.ratify_date)
        --   OR (psma_flag (port_flag) AND timestamp > d.ratify_date AND psma_flag (flag) AND timestamp > c.ratify_date) AS PSMA_group_visit,

        psma_flag (port_flag) AND IFNULL (timestamp > d.ratify_date, FALSE) AS psma_port,
        fishing_hours,
        event_start
      FROM fishing_events AS a
      JOIN (
        SELECT *,
          # If previous event is not available, put it as 1 year ago
          IFNULL (
            LAG (event_end) OVER (PARTITION BY ssvid ORDER BY event_end),
            TIMESTAMP_SUB (event_end, INTERVAL 365 DAY)) AS prev_event_end
        FROM target_vessel_landings ) AS b
      ON a.ssvid = b.ssvid
        AND a.timestamp BETWEEN b.prev_event_end AND b.event_start
      LEFT JOIN ratify_info AS c
      ON group_eu_flags (flag) = c.ratify_flag
      LEFT JOIN ratify_info AS d
      ON group_eu_flags (port_flag) = d.ratify_flag
      # Cut off if the previous event start was too long ago, 1 year cut-off
      WHERE flag IS NOT NULL AND port_flag IS NOT NULL
        AND a.timestamp > TIMESTAMP_SUB (b.event_start, INTERVAL 365 DAY) )
    GROUP BY 1,2,3,4,5
  ),

  -------------------------------------------------------------
  -- Group landed fishing effort by PSMA fleet and by PSMA port
  -------------------------------------------------------------
  sumup AS (
    SELECT
      psma,
      timeline,
      SUM (IF (PSMA_group_visit, total_fishing_hours, 0)) AS fishing_effort_landed_psma_group,
      SUM (IF (PSMA_group_visit AND dom_landing, total_fishing_hours, 0)) AS fishing_effort_landed_domestic,
      SUM (total_fishing_hours) AS fishing_effort_landed_total,
      SAFE_DIVIDE (SUM (IF (PSMA_group_visit, total_fishing_hours, 0)), SUM (total_fishing_hours)) AS frac_psma_group,
      SAFE_DIVIDE (SUM (IF (PSMA_group_visit AND dom_landing, total_fishing_hours, 0)), SUM (total_fishing_hours)) AS frac_domestic,
      SAFE_DIVIDE (SUM (IF (PSMA_group_visit AND NOT dom_landing, total_fishing_hours, 0)), SUM (total_fishing_hours)) AS frac_psma_group_not_domestic,
      SUM (SUM (total_fishing_hours)) OVER (PARTITION BY psma) AS sum_total,
      SUM (IF (PSMA_group_visit, landing_count, 0)) AS landing_count_psma_group,
      SUM (IF (PSMA_group_visit AND dom_landing, landing_count, 0)) AS landing_count_domestic,
      SUM (landing_count) AS landing_count_total,
      SUM (IF (PSMA_group_visit, landing_count, 0)) / SUM (landing_count) AS frac_count,
      SAFE_DIVIDE (SUM (IF (PSMA_group_visit, total_fishing_hours, 0)), SUM (IF (PSMA_group_visit, landing_count, 0))) AS avg_catch_per_landing
    FROM combined
    GROUP BY 1,2
  )

SELECT *
FROM sumup
ORDER BY psma, timeline
