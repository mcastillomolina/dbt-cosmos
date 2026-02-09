{{
    config(
        materialized='table',
        tags=['analytics', 'summary']
    )
}}

select
    incident_type,
    count(*) as total_incidents,
    count(case when resolved then 1 end) as resolved_incidents,
    count(case when not resolved then 1 end) as unresolved_incidents,
    round(avg(impact_score), 2) as avg_impact_score,
    round(avg(case when resolved then resolution_time_hours end), 2) as avg_resolution_hours,
    min(incident_date) as first_incident_date,
    max(incident_date) as last_incident_date
from {{ ref('stg_cybersecurity_incidents') }}
group by incident_type
order by total_incidents desc
