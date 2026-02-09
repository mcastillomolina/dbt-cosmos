{{
    config(
        materialized='table',
        tags=['analytics', 'regional']
    )
}}

select
    region,
    industry,
    count(*) as total_incidents,
    round(avg(impact_score), 2) as avg_impact_score,
    count(case when severity = 'Critical' then 1 end) as critical_incidents,
    count(case when resolved then 1 end) as resolved_count,
    round(100.0 * count(case when resolved then 1 end) / count(*), 2) as resolution_rate_pct
from {{ ref('stg_cybersecurity_incidents') }}
group by region, industry
order by total_incidents desc
