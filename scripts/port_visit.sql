#standardSQL

with

-- port visit from 2016
-- excluding visit < 3 hours
port_visit as (
    select
        ssvid,
        start_timestamp,
        end_timestamp,
        start_anchorage_id as s2id
    from (
      SELECT
        JSON_EXTRACT_SCALAR (event_vessels, "$[0].ssvid") AS ssvid,
        event_start AS start_timestamp,
        event_end AS end_timestamp,
        JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.anchorage_id") AS anchorage_id,
        JSON_EXTRACT_SCALAR (event_info, "$.start_anchorage.anchorage_id") AS start_anchorage_id,
        SAFE_CAST (JSON_EXTRACT_SCALAR (event_info, "$.confidence") AS INT64) AS confidence
      FROM `world-fishing-827.pipe_production_v20201001.published_events_port_visits` )
    where anchorage_id != '10000001'
        and confidence = 4
        and start_timestamp >= '2016-01-01'
        and timestamp_diff(end_timestamp, start_timestamp, second)/3600 >= 3
    group by 1,2,3,4
),

-- add anchorage label & remove Panama Canal
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
            sublabel,
            iso3
        from `world-fishing-827.anchorages.named_anchorages_v20220511`
    )
    using (s2id)
    where sublabel is null
        or (sublabel != 'PANAMA CANAL'
          AND label != "SUEZ" AND label != "SUEZ CANAL")
),

-- add fishing vessel info
port_visit_fishing as (
    select * except (x) 
    from port_visit_label as a
    -- left join (
    LEFT JOIN (
        select
            ssvid as x,
            year,
            best_flag as fishing_flag,
            'fishing' as vessel_class1,
            best_vessel_class as gear_type1
        from `world-fishing-827.gfw_research.fishing_vessels_ssvid_v20230101`
    ) as b
    on a.ssvid = b.x
        and extract(year from start_timestamp) = year
),

-- list of reefer & bunker vessels
reefer_bunker_list as (
    select DISTINCT
        identity.ssvid as ssvid,
        identity.flag as flag,
        (SELECT MIN (first_timestamp) FROM UNNEST (activity)) AS first_timestamp,
        (SELECT MAX (last_timestamp) FROM UNNEST (activity)) AS last_timestamp,
        gear_type as gear_type2,
        case
            when gear_type in ('specialized_reefer', 'reefer', 'fish_factory') then 'carrier'
            when gear_type = 'bunker' then 'bunker'
            else null
        end as vessel_class2
    from `world-fishing-827.vessel_database.all_vessels_v20230101`,
    unnest (feature.geartype) AS gear_type
    where identity.ssvid not in ('888888888', '0')
        and ((is_carrier and gear_type in ('specialized_reefer', 'reefer', 'fish_factory'))
        or (is_bunker and gear_type = 'bunker'))
),


-- add reefers and bunkers to port visit
port_visit_reefer_bunker as (
    select * except (x) 
    from port_visit_fishing as a
    left join (
        select
            ssvid as x,
            flag,
            vessel_class2,
            gear_type2,
            first_timestamp,
            last_timestamp
        from reefer_bunker_list
        ) as b
    on a.ssvid = b.x
        and b.first_timestamp <= a.start_timestamp
        and a.end_timestamp <= b.last_timestamp
),

final as (
    select DISTINCT
        ssvid,
        start_timestamp,
        iso3,
        if(vessel_class1='fishing', fishing_flag, flag) as flag,
        if(vessel_class1='fishing', vessel_class1, vessel_class2) as vessel_class,
        if(vessel_class1='fishing', gear_type1, gear_type2) as gear_type
    from port_visit_reefer_bunker
)

select
    ssvid,
    date(start_timestamp) as visit_date,
    iso3,
    flag,
    vessel_class,
    gear_type
from final
where vessel_class is not null