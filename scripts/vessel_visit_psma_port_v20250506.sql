#standardSQL

with


-- port visit in 2024
-- excluding visit < 3 hours
port_visit as (
    select
        event_id,
        ssvid,
        extract(year from start_timestamp) as year,
        start_timestamp,
        end_timestamp,
        start_anchorage_id as s2id
    from (
      SELECT
        event_id,
        JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
        event_start AS start_timestamp,
        event_end AS end_timestamp,
        JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.anchorage_id") AS start_anchorage_id,
        SAFE_CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) AS confidence
      FROM `world-fishing-827.pipe_production_v20201001.published_events_port_visits` )
    where start_anchorage_id != '10000001'
        and confidence = 4
        and extract(year from start_timestamp) = 2024
        and timestamp_diff(end_timestamp, start_timestamp, second)/3600 >= 3
    group by 1,2,3,4,5,6
),


-- add anchorage label & remove Panama & Suez Canal
port_visit_label as (
    select * from port_visit
    left join (
        select
            s2id,
            case
                when label = 'SU-AO' then 'SUAO'
                when label = 'SAINT PETERSBURG' then 'ST PETERSBURG'
                else label
            end as label,
            iso3
        from `world-fishing-827.anchorages.named_anchorages_v20240117`
        where sublabel != 'PANAMA CANAL'
            or label != 'SUEZ CANAL'
    )
    using (s2id)
),


-- add pacific
port_visit_pacific as (
    select
        * except(is_pacific_rim),
        if(is_pacific_rim is null, 0, 1) as is_pacific_rim
    from port_visit_label
    left join (
        select *
        from `gfwanalysis.pacific.s2id_pacific_rim` 
    )
    using(s2id)
),


-- vessel_info: fishing, carrier (no fish factory, well boat), bunker 
vessel_info as (
    select distinct
        ssvid,
        year,
        if(best.best_flag = 'UNK', null, best.best_flag) as flag,
        case
            when on_fishing_list_best then 'fishing'
            when best.best_vessel_class in ('reefer', 'specialized_reefer', 'container_reefer') then 'carrier'
            when best.best_vessel_class in ('bunker', 'tanker', 'bunker_or_tanker') then 'bunker'
            else null
        end as vessel_class,
        case
            when best.best_vessel_class is null and on_fishing_list_best then 'fishing'
            else best.best_vessel_class
        end as gear_type
    from `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v`
    where on_fishing_list_best
        or best.best_vessel_class in ('reefer', 'specialized_reefer', 'container_reefer', 'bunker', 'tanker', 'bunker_or_tanker')
),


-- port visit by fishing & support vessels
port_visit_vessel as (
    select *
    from port_visit_pacific
    left join vessel_info
    using(ssvid, year)
    where vessel_class is not null
),


-- add previous port visit ending time
port_visit_prev as (
    select
        * except(is_pacific_rim),
        if(is_pacific_rim is null, 0, 1) as is_pacific_rim,
        lag(end_timestamp) over (partition by ssvid order by end_timestamp asc) as prev_end_timestamp
    from port_visit_vessel
),


--------------------------------------
-- fishing
--------------------------------------
fishing_event as (
  select
    JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS x,
    event_start,
    event_end,
    if(high_seas is null, 0, 1) as is_high_seas
  from
    `world-fishing-827.pipe_production_v20201001.published_events_fishing`
  left join unnest(regions_mean_position.high_seas) as high_seas
  where event_start between '2021-01-01' and '2025-01-01'
),


-- add number of fishing events to each port visit per vessel
port_visit_fishing_event as (
    select
        event_id,
        ssvid,
        flag,
        vessel_class,
        gear_type,
        coalesce(sum(is_high_seas), 0) as n_fishing_hs,
        sum(case when is_high_seas is null then 0 else 1 end) as n_fishing_all
    from
        port_visit_prev as a
    left join (select * from fishing_event) as b
    on a.ssvid = b.x
        and a.prev_end_timestamp < b.event_start
        and b.event_end < a.start_timestamp
    where vessel_class = 'fishing'
    group by 1,2,3,4,5
),


--------------------------------------
-- encounter
--------------------------------------
encounter_event as (
  select
    JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS x,
    event_start,
    event_end,
    if(high_seas is null, 0, 1) as is_high_seas
  from
    `world-fishing-827.pipe_production_v20201001.published_events_encounters`
  left join unnest(regions_mean_position.high_seas) as high_seas
  where event_start between '2021-01-01' and '2025-01-01'
),


-- add number of encounter events to each port visit per vessel
port_visit_encounter_event as (
    select
        event_id,
        ssvid,
        flag,
        vessel_class,
        gear_type,
        coalesce(sum(is_high_seas), 0) as n_encounters_hs,
        sum(case when is_high_seas is null then 0 else 1 end) as n_encounters_all
    from
        port_visit_prev as a
    left join (select * from encounter_event) as b
    on a.ssvid = b.x
        and a.prev_end_timestamp < b.event_start
        and b.event_end < a.start_timestamp
    group by 1,2,3,4,5
),


--------------------------------------
-- loitering
--------------------------------------
loitering_event as (
  select
    JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS x,
    event_start,
    event_end,
    if(high_seas is null, 0, 1) as is_high_seas
  from
    `world-fishing-827.pipe_production_v20201001.published_events_loitering`
  left join unnest(regions_mean_position.high_seas) as high_seas
  where event_start between '2021-01-01' and '2025-01-01'
),


-- add number of loitering events to each port visit per vessel
port_visit_loitering_event as (
    select
        event_id,
        ssvid,
        flag,
        vessel_class,
        gear_type,
        coalesce(sum(is_high_seas), 0) as n_loitering_hs,
        sum(case when is_high_seas is null then 0 else 1 end) as n_loitering_all
    from
        port_visit_prev as a
    left join (select * from encounter_event) as b
    on a.ssvid = b.x
        and a.prev_end_timestamp < b.event_start
        and b.event_end < a.start_timestamp
    group by 1,2,3,4,5
),


--------------------------------------
-- combine all events
--------------------------------------
port_visit_event as (
    select
        event_id,
        ssvid,
        flag,
        vessel_class,
        gear_type,
        sum(n_fishing_all) as n_fishing_all,
        sum(n_fishing_hs) as n_fishing_hs,
        sum(n_encounters_all) as n_encounters_all,
        sum(n_encounters_hs) as n_encounters_hs,
        sum(n_loitering_all) as n_loitering_all,
        sum(n_loitering_hs) as n_loitering_hs
    from port_visit_fishing_event
    full outer join (select * from port_visit_encounter_event)
    using(event_id, ssvid, flag, vessel_class, gear_type)
    full outer join (select * from port_visit_loitering_event)
    using(event_id, ssvid, flag, vessel_class, gear_type)
    group by 1,2,3,4,5
),


-- add port name
port_visit_port_info as (
    select *
    from port_visit_event
    left join (
        select event_id, iso3, label, is_pacific_rim
        from port_visit_pacific
    )
    using(event_id)
),


-- add visit year & date 
port_visit_date as (
    select *
    from port_visit_port_info
    left join (
        select 
            extract(year from start_timestamp) as year,
            extract(date from start_timestamp) as date,
            event_id
        from port_visit_vessel

    )
    using(event_id)
),


--------------------------------------------------
-- add GFW anchorage s2id to PSMA designated ports
--------------------------------------------------
-- PSMA designated ports
psma_port as (
    select
        country,
        port_name,
        lat as port_lat,
        lon as port_lon,
        st_geogpoint(lon, lat) AS port_coords,
        row_number() over(order by country, port_name) as port_id
    from
        `gfwanalysis.misc.psma_designated_port_20250501`
    where
        lat is not null
),


-- GFW anchorage table
gfw_anchorage as (
    select
        label,
        s2id,
        iso3,
        lat as anchorage_lat,
        lon as anchorage_lon,
        st_geogpoint(lon, lat) as anchorage_coords
    from
        `world-fishing-827.anchorages.named_anchorages_v20240117`
),


-- merge within 3 km
psma_port_anchorage as (
    select
        array_agg(port_id order by st_distance(anchorage_coords, port_coords)) [ordinal(1)] as port_id,
        s2id
    from psma_port
    join gfw_anchorage
    on st_dwithin(anchorage_coords, port_coords, 3000) -- search within 3 km
    group by s2id
),


-- add coordinates
psma_port_coords as (
    select
        port_id,
        s2id,
        country,
        port_name,
        port_lat,
        port_lon,
        port_coords,
        label,
        iso3,
        anchorage_coords
    from
        psma_port_anchorage
    left join (select * from psma_port)
    using (port_id)
    left join (select * from gfw_anchorage)
    using (s2id)
),


-- add distance between port and anchorage
psma_port_distance as (
    select
        *,
        st_distance(port_coords, anchorage_coords)/1000 as distance_km
    from
        psma_port_coords
),


-- rank distance
psma_port_distance_ranked as (
    select
        *,
        row_number() over(partition by port_id order by distance_km asc) as rank
    from
        psma_port_distance
),


-- get the shortest
psma_port_label as (
   select * except(rank)
   from psma_port_distance_ranked
   where rank = 1
),


-- add dates 
psma_port_date as (
    select *
    from psma_port_label
    left join (
        select *
        from `gfwanalysis.misc.psma_ratifier_full_v20250416`
    )
    using(iso3)
),


--------------------------------------------------
-- add PSMA ports to port visit
--------------------------------------------------
port_visit_psma as (
    select
        *,
        case 
            when date >= Entry_into_force_date then 1
            when date >= '2016-06-05' and iso3 in ('USA', 'ASM', 'VIR', 'GUM', 'MNP', 'UMI', 'PRI') then 1
            else 0
        end as is_psma_port
    from port_visit_date
    left join (
        select port_name, label, iso3, Entry_into_force_date
        from psma_port_date
    )
    using(iso3, label)
)


select
    year,
    date,
    event_id,
    ssvid,
    flag,
    vessel_class,
    gear_type,
    port_name,
    label,
    iso3,
    is_pacific_rim,
    coalesce(is_psma_port, 0) as is_psma_port,
    coalesce(n_fishing_all, 0) as n_fishing_all,
    coalesce(n_fishing_hs, 0) as n_fishing_hs,
    coalesce(n_encounters_all, 0) as n_encounters_all,
    coalesce(n_encounters_hs, 0) as n_encounters_hs,
    coalesce(n_loitering_all, 0) as n_loitering_all,
    coalesce(n_loitering_hs, 0) as n_loitering_hs
from port_visit_psma


-- save to BigQUery gfwanalysis.misc.vessel_visit_psma_port_v20250506
