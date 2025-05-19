with


-- from vessel_visit_psma_port_v20250429.sql
port_visit as (
    select *
    from `gfwanalysis.misc.vessel_visit_psma_port_v20250506`
    left join (
        select
            sovereign_iso3,
            territory_iso3 as iso3
        from `gfwanalysis.misc.sovereign_territory_pairs`)
    using(iso3)
    left join (
        select
            sovereign_iso3 as sovereign_flag,
            territory_iso3 as flag
        from `gfwanalysis.misc.sovereign_territory_pairs`)
    using(flag)
),


port_visit_sovereign as (
    select
        * except(sovereign_iso3, sovereign_flag),
        if(sovereign_iso3 is null, iso3, sovereign_iso3) as sovereign_iso3,
        if(sovereign_flag is null, flag, sovereign_flag) as sovereign_flag
    from port_visit
),


port_visit_domestic as (
    select
        *,
        case
            when sovereign_iso3 = sovereign_flag  then 'domestic'
            when (year < 2021) and (iso3 in ('GBR', 'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE') and flag in ('GBR', 'AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE')) then 'domestic'
            when (year >= 2021) and (iso3 in ('AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE') and flag in ('AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE')) then 'domestic'
            else 'foreign'
        end as type
    from port_visit_sovereign       
),


-- add psma state
port_visit_psma as (
    select
        a.*,
        if(year >= psma_year, 1, 0) as is_psma_state
    from port_visit_domestic as a
    left join (
        select
            iso3,
            extract(year from Entry_into_force_date) as psma_year
        from `gfwanalysis.misc.psma_ratifier_full_v20250416`
    ) as b
    using (iso3)
),


summary as (
    select
        year,
        iso3,
        port_name,
        label,
        vessel_class,
        type,
        flag,
        is_pacific_rim,
        is_psma_port,
        is_psma_state,
        count(*) as n_visits,
        sum(case when vessel_class = 'fishing' then 1 else 0 end) as n_visits_fishing_vessel,
        sum(case when (vessel_class = 'fishing' and n_fishing_hs > 0) then 1 else 0 end) as n_visits_fishing_vessel_hs,
        sum(case when vessel_class in ('carrier', 'bunker') then 1 else 0 end) as n_visits_support_vessel,
        sum(case when (vessel_class in ('carrier', 'bunker') and (n_encounters_hs > 0 or n_loitering_hs > 0)) then 1 else 0 end) as n_visits_support_vessel_hs
    from port_visit_psma
    group by 1,2,3,4,5,6,7,8,9,10
)

select * from summary

