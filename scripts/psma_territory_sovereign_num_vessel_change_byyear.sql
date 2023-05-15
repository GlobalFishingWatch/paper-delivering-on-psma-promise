WITH
  sov_ter AS (
    SELECT DISTINCT territory1_iso3, sovereign1_iso3
    FROM `gfw_research.eez_info`
    WHERE territory1_iso3 IS NOT NULL
      AND sovereign1_iso3 != "NA"
      AND sovereign1_iso3 IN ('DNK') #'FRA', 'GBR', 'DNK', 'NOR')
      AND sovereign1_iso3 != territory1_iso3
  ),

  fishing_vessels AS (
    SELECT ssvid, best.best_flag,
      EXTRACT (YEAR FROM activity.first_timestamp) AS year_start,
      EXTRACT (YEAR FROM activity.last_timestamp) AS year_end
    FROM `gfw_research.vi_ssvid_v20221001`
    WHERE on_fishing_list_best
  ),

  years AS (
    SELECT year
    FROM UNNEST (GENERATE_ARRAY (2012, 2022, 1)) AS year
  ),

  combined AS (
    SELECT *
    FROM years
    LEFT JOIN (
      SELECT *
      FROM fishing_vessels )
    ON year BETWEEN year_start AND year_end
    JOIN sov_ter
    ON (territory1_iso3 = best_flag)
  )

SELECT year, best_flag, COUNT (*) AS cnt, #sovereign1_iso3, COUNT (*) AS cnt
FROM combined
GROUP BY 1,2
ORDER BY 2,1