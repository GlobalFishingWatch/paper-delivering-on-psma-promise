#standardSQL


-- range of the period of interest
create temp function minimum() as (timestamp('2015-01-01'));
create temp function maximum() as (timestamp('2021-12-31'));


with


-- fishing events on the high seas
fishing_event as (
    select distinct
        vessel_id,
        event_id,
        event_start,
        event_end,
        extract(year from event_start) as year
    from
        `world-fishing-827.pipe_ais_v3_published.product_events_fishing`
    left join unnest(regions_mean_position.high_seas) as high_seas
    where event_start between minimum() and maximum()
        and high_seas is not null
),


-- filter for vessels on the fishing vessel list
fishing_event_clean as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        'fishing' as event_type
    from fishing_event
    left join (
        select vessel_id, ssvid
        from `world-fishing-827.pipe_ais_v3_published.vessel_info`
    )
    using(vessel_id)
    left join (
        select ssvid, year, on_fishing_list_best
        from `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v`
    )
    using(year, ssvid)
    where on_fishing_list_best
),


-- encounter events with a standard filter
encounter_event as (
    select distinct
        vessel_id,
        event_id,
        event_start,
        event_end,
        json_extract_scalar (event_vessels, "$[1].id") as vessel_id_encountered,
        json_extract_scalar (event_vessels, "$[1].ssvid") as ssvid_encountered,
        'encounter' as event_type
    from
        `world-fishing-827.pipe_ais_v3_published.product_events_encounter`
    where event_start between minimum() and maximum()
        and safe_cast(json_extract_scalar(event_info, "$.median_distance_km") as float64) < 0.5
        and safe_cast(json_extract_scalar(event_info, "$.median_speed_knots") as float64) < 2
        and timestamp_diff(event_end, event_start, minute)/60 > 2
),


-- encounter with support
encounter_event_support as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        event_type,
    from encounter_event as a
    left join (
        select distinct
            identity.ssvid as ssvid,
            (select min (first_timestamp) from unnest (activity)) as first_timestamp,
            (select max (last_timestamp) from unnest (activity)) as last_timestamp,
            case
                when is_carrier and gear_type in ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory') then 'carrier'
                when is_bunker and gear_type in ('bunker', 'tanker', 'bunker_or_tanker') then 'bunker'
                else null 
            end as vessel_class
        from
            `world-fishing-827.vessel_database.all_vessels`
            left join unnest(feature.geartype) as gear_type
        where
            identity.ssvid not in ('888888888', '0')
    ) as b
    on a.ssvid_encountered = b.ssvid
        and b.first_timestamp < a.event_start
        and (a.event_end < b.last_timestamp or b.last_timestamp is null)
    where vessel_class in ('carrier', 'bunker')
),


-- port visit_events > 3 hours, confidence >= 3
port_visit_event as (
    select distinct
        vessel_id,
        event_id,
        event_start,
        event_end,
        s2id,
        extract(year from event_start) as year
    from (
        select
            *,
            json_extract_scalar (event_info, "$.start_anchorage.anchorage_id") as s2id,
            safe_cast (json_extract_scalar (event_info, "$.confidence") as int64) as confidence
        from
            `world-fishing-827.pipe_ais_v3_published.product_events_port_visit`
        where
            event_start between minimum() and maximum()
    )
    where s2id != '10000001'
        and confidence >= 3
        and timestamp_diff(event_end, event_start, minute)/60 > 3
),


-- remove Panama Canal, Suez Canal & Singapore
port_visit_event_clean as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        'port_visit' as event_type
    from port_visit_event
    left join (
        select s2id, sublabel
        from `world-fishing-827.anchorages.named_anchorages_v20240117`
    )
    using (s2id)
    where sublabel is null
        or sublabel not in ('PANAMA CANAL', "SUEZ CANAL", "SINGAPORE")
),


-- combine
all_events as (
    select * from fishing_event_clean
    union all
    select * from encounter_event_support
    where vessel_id in (select vessel_id from fishing_event_clean)
    union all 
    select * from port_visit_event_clean
    where vessel_id in (select vessel_id from fishing_event_clean)
),

-- remove fishing events > 1 year from the subsequent port visit or encounter
all_events2 as (
    select
        *,
        sum(case when event_type in ('encounter', 'port_visit') then 1 else 0 end) over (
            partition by vessel_id
            order by event_start desc
        ) as x
    from all_events
),

all_events_landing_time as (
    select
        * except(x),
        max(event_start) over (partition by vessel_id, x) as landing_time
    from all_events2
),

all_events_filtered as (
    select * except(landing_time), extract(year from event_start) as year
    from all_events_landing_time
    where not (event_type = 'fishing' and timestamp_diff(landing_time, event_end, minute)/60 > 24*365)
),


--------------------------------------------
-- port visits by fishing vessels
--------------------------------------------
-- add reset count
all_events_reset_count as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        event_type,
        sum(case when event_type = 'port_visit' then 1 else 0 end) over (
                partition by vessel_id
                order by event_start
            ) as reset_count
    from all_events_filtered
),


-- cumulative sum with reset logic
all_events_high_seas_fishing as (
    select
        *,
        sum(case when event_type = 'fishing' then 1 else 0 end) over (
            partition by vessel_id, reset_count
            order by event_start asc
            rows between unbounded preceding and current row
        ) as n_high_seas_fishing
    from all_events_reset_count
),


all_events_landed_fishing as (
    select
        *,
        lag(n_high_seas_fishing) over (
            partition by vessel_id
            order by event_start asc
        ) as x
    from all_events_high_seas_fishing
),


port_visit_high_seas_fishing as (
    select *
    from all_events_landed_fishing
    where event_type = 'port_visit'
        and x > 0
),



---------------------------
-- add vessel flag, port iso3 and year
---------------------------
port_visit_high_seas_fishing_info as (
    select
        vessel_id,
        event_id,
        iso3,
        flag,
        extract(year from event_start) as year
    from port_visit_event
    left join (
        select s2id, iso3
        from `world-fishing-827.anchorages.named_anchorages_v20240117`
    )
    using (s2id)
    left join (
        select vessel_id, ssvid
        from `world-fishing-827.pipe_ais_v3_published.vessel_info`
    )
    using (vessel_id)
    left join (
        select ssvid, year, if(best.best_flag = 'UNK', null, best.best_flag) as flag
        from `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v`
    )
    using(ssvid, year)
    where event_id in (select event_id from port_visit_high_seas_fishing)
)


select
    iso3,
    flag,
    year,
    count(*) as n_visits
from port_visit_high_seas_fishing_info
group by 1,2,3