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
        json_extract_scalar (event_vessels, "$[1].ssvid") as ssvid_encountered
    from
        `world-fishing-827.pipe_ais_v3_published.product_events_encounter`
    where event_start between minimum() and maximum()
        and safe_cast(json_extract_scalar(event_info, "$.median_distance_km") as float64) < 0.5
        and safe_cast(json_extract_scalar(event_info, "$.median_speed_knots") as float64) < 2
        and timestamp_diff(event_end, event_start, minute)/60 > 2
),


-- encounter with carrier
encounter_event_carrier as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        'encounter' as event_type
    from encounter_event as a
    left join (
        select distinct
            identity.ssvid as x,
            (select min (first_timestamp) from unnest (activity)) as first_timestamp,
            (select max (last_timestamp) from unnest (activity)) as last_timestamp,
            1 as carrier
        from
            `world-fishing-827.vessel_database.all_vessels`
            left join unnest(feature.geartype) as gear_type
        where
            identity.ssvid not in ('888888888', '0')
            and (is_carrier and gear_type in ('reefer', 'specialized_reefer', 'container_reefer', 'fish_factory'))
    ) as b
    on a.ssvid_encountered = b.x
        and b.first_timestamp < a.event_start
        and (a.event_end < b.last_timestamp or b.last_timestamp is null)
    where carrier = 1
),


-- port visit_events > 3 hours, confidence >= 3
port_visit_event as (
    select distinct
        vessel_id,
        event_id,
        event_start,
        event_end,
        s2id
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
    select * from encounter_event_carrier
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
-- landed fishing effort by fishing vessels
--------------------------------------------
-- add fishing effort
all_events_fishing_effort as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        event_type,
        engine_power_kw,
        case
            when event_type = 'fishing' then timestamp_diff(event_end, event_start, minute)/60
            else 0
        end as fishing_h,
        sum(case when event_type in ('encounter', 'port_visit') then 1 else 0 end) over (
                    partition by vessel_id
                    order by event_start
                ) as reset_count
    from all_events_filtered
    left join (
        select vessel_id, ssvid
        from `world-fishing-827.pipe_ais_v3_published.vessel_info`
    )
    using (vessel_id)
    left join (
        select
            ssvid,
            year,
            best.best_engine_power_kw as engine_power_kw
        from `world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v`
    )
    using(ssvid, year)
),


-- cumulative sum with reset logic
fishing_effort_cumsum as (
    select
        *,
        sum(fishing_h) over (
            partition by vessel_id, reset_count
            order by event_start asc
            rows between unbounded preceding and current row
        ) as cumulative_fishing_h,
        sum(fishing_h * engine_power_kw) over (
            partition by vessel_id, reset_count
            order by event_start asc
            rows between unbounded preceding and current row
        ) as cumulative_fishing_effort
    from all_events_fishing_effort
),


-- landed fishing effort
fishing_effort_cumsum2 as (
    select
        *,
        lag(cumulative_fishing_h) over (
            partition by vessel_id
            order by event_start asc
        ) as landed_fishing_h,
        lag(cumulative_fishing_effort) over (
            partition by vessel_id
            order by event_start asc
        ) as landed_fishing_effort
    from fishing_effort_cumsum
),

fishing_effort_landed as (
    select *
    from fishing_effort_cumsum2
    where event_type = 'port_visit'
),


--------------------------------------------
-- landed fishing effort by carrier vessels
--------------------------------------------
-- fishing effort transferred to carrier
transferred_fishing_event as (
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        event_type,
        landed_fishing_h as transferred_fishing_h,
        landed_fishing_effort as transferred_fishing_effort
    from fishing_effort_cumsum2
    where event_type = 'encounter'
),


-- add carrier vessel id
transferred_fishing_event_carrier as (
    select
        vessel_id_encountered as vessel_id,
        event_id,
        event_start,
        event_end,
        event_type,
        transferred_fishing_h,
        transferred_fishing_effort,
    from transferred_fishing_event
    left join (
        select event_id, vessel_id_encountered
        from encounter_event
    )
    using(event_id)
),


-- add port visit
all_events_carrier as (
    select * from transferred_fishing_event_carrier
    union all 
    select
        vessel_id,
        event_id,
        event_start,
        event_end,
        event_type,
        0 as transferred_fishing_h,
        0 as transferred_fishing_effort,
    from port_visit_event_clean
    where vessel_id in (select vessel_id from transferred_fishing_event_carrier)
),


-- add reset_count
all_events_fishing_effort_carrier as (
    select
        *,
        sum(case when event_type = 'port_visit' then 1 else 0 end) over (
                    partition by vessel_id
                    order by event_start
                ) as reset_count
    from all_events_carrier
),


-- cumulative sum with reset logic
transferred_fishing_effort_cumsum as (
    select
        *,
        sum(transferred_fishing_h) over (
            partition by vessel_id, reset_count
            order by event_start asc
            rows between unbounded preceding and current row
        ) as cumulative_fishing_h,
        sum(transferred_fishing_effort) over (
            partition by vessel_id, reset_count
            order by event_start asc
            rows between unbounded preceding and current row
        ) as cumulative_fishing_effort
    from all_events_fishing_effort_carrier
),


-- landed fishing effort by carrier vessels
transferred_fishing_effort_cumsum2 as (
    select
        *,
        lag(cumulative_fishing_h) over (
            partition by vessel_id
            order by event_start asc
        ) as landed_fishing_h,
        lag(cumulative_fishing_effort) over (
            partition by vessel_id
            order by event_start asc
        ) as landed_fishing_effort
    from transferred_fishing_effort_cumsum
),

fishing_effort_landed_carrier as (
    select *
    from transferred_fishing_effort_cumsum2
    where event_type = 'port_visit'
),


--------------------------------------------
-- combine
--------------------------------------------
fishing_effort_landed_all as (
    select
        vessel_id,
        event_id,
        extract(year from event_start) as year,
        landed_fishing_h,
        landed_fishing_effort,
        'fishing' as donor
    from fishing_effort_landed
    union all
    select
        vessel_id,
        event_id,
        extract(year from event_start) as year,
        landed_fishing_h,
        landed_fishing_effort,
        'carrier' as donor
    from fishing_effort_landed_carrier
),


-- add flag
fishing_effort_landed_flag as (
    select *
    from fishing_effort_landed_all
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
),


-- add port iso3
fishing_effort_landed_port as (
    select distinct *
    from fishing_effort_landed_flag
    left join (
        select event_id, s2id
        from port_visit_event
    )
    using(event_id)
    left join (
        select s2id, iso3
        from `world-fishing-827.anchorages.named_anchorages_v20240117`
    )
    using (s2id)
)


select
    flag,
    iso3,
    year,
    donor,
    sum(landed_fishing_h) as landed_fishing_h,
    sum(landed_fishing_effort) as landed_fishing_effort
from fishing_effort_landed_port
group by year, flag, donor, iso3
