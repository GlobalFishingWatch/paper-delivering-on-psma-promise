#standardSQL


WITH


-- This subquery identifies good segments
good_segments AS (
    SELECT
        seg_id
    FROM
        `world-fishing-827.pipe_production_v20201001.research_segs`
    WHERE good_seg
        AND NOT overlapping_and_short
),


-- Get the list of active fishing vessels that pass the noise filters
fishing_vessels AS (
    SELECT
        ssvid,
        best_vessel_class,
        year
    FROM `world-fishing-827.gfw_research.fishing_vessels_ssvid`
    where year  = 2021
),


-- add vessel power
power as (
    select
        ssvid,
        year,
        best.best_engine_power_kw as engine_power_kw
    from `world-fishing-827.gfw_research.vi_ssvid_byyear`
    where year = 2021
),


fishing_vessel_power as (
    select * from fishing_vessels
    left join (
        select * from power
    )
    using(year, ssvid)
),


-- fishing hours
fishing AS (
SELECT
    ssvid,
    FLOOR(lat / 5) as lat_bin,
    FLOOR(lon / 5) as lon_bin,
    EXTRACT(year FROM _partitiontime) as year,
    hours,
    nnet_score,
    night_loitering,
    eez
FROM `world-fishing-827.pipe_production_v20201001.research_messages`
LEFT JOIN UNNEST(regions.eez) AS eez
WHERE _partitiontime BETWEEN "2021-01-01" AND '2022-01-01'
AND is_fishing_vessel
AND seg_id IN (SELECT seg_id FROM good_segments)
),


-- Filter fishing to just the list of active fishing vessels in that year
-- Create fishing_score attribute using night_loitering instead of nnet_score
-- for squid jiggers
fishing_filtered AS (
    SELECT
        *,
        IF(best_vessel_class = 'squid_jigger',
            night_loitering,
            nnet_score) as fishing_score
        FROM fishing
    JOIN fishing_vessels
    USING(ssvid, year)
),


-- Calculate fishing hours using combined fishing score metric
fishing_hours_filtered AS (
    SELECT
        *,
        IF(fishing_score > 0.5, hours, 0) as fishing_hours
    FROM fishing_filtered
),


-- add power
fishing_hours_filtered_power as (
    select *
    from fishing_hours_filtered
    left join (
        select * from fishing_vessel_power
    )
    using(year, ssvid)
)


SELECT
    lat_bin * 5 as lat_bin,
    lon_bin * 5 as lon_bin,
    sum(fishing_hours) as fishing_hours,
    sum(fishing_hours * engine_power_kw) as kw_fishing_hours
FROM fishing_hours_filtered_power
GROUP BY lat_bin, lon_bin
