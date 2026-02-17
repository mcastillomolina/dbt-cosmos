{{
    config(
        materialized='view',
        tags=['staging', 'weekly', 'cybersecurity']
    )
}}

with source_data as (
    select * from {{ ref('cybersecurity_incidents') }}
),

cleaned as (
    select
        incident_id,
        incident_date,
        incident_type,
        severity,
        affected_systems,
        source_ip,
        destination_ip,
        attack_vector,
        impact_score,
        resolved,
        resolution_time_hours,
        detected_by,
        region,
        industry,
        current_timestamp() as loaded_at
    from source_data
)

select * from cleaned
