{{
    config(
        materialized='table',
        tags=['analytics', 'critical']
    )
}}

select
    incident_id,
    incident_date,
    incident_type,
    severity,
    affected_systems,
    impact_score,
    resolved,
    resolution_time_hours,
    region,
    industry
from {{ ref('stg_cybersecurity_incidents') }}
where severity = 'Critical'
order by incident_date desc, impact_score desc
