{{
    config(
        materialized='view',
        tags=['staging', 'weekly', 'premier_league']
    )
}}

with source_data as (
    select * from {{ ref('premier_league_matches') }}
),

cleaned as (
    select
        match_id,
        match_date,
        season,
        week,
        home_team,
        away_team,
        home_score,
        away_score,
        home_score + away_score as total_goals,
        case
            when home_score > away_score then home_team
            when away_score > home_score then away_team
            else 'Draw'
        end as winner,
        case
            when home_score > away_score then 'Home Win'
            when away_score > home_score then 'Away Win'
            else 'Draw'
        end as result_type,
        stadium,
        attendance,
        referee,
        current_timestamp() as loaded_at
    from source_data
)

select * from cleaned
