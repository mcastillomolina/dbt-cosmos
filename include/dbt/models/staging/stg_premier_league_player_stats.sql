{{
    config(
        materialized='view',
        tags=['staging', 'weekly', 'premier_league']
    )
}}

with source_data as (
    select * from {{ ref('premier_league_player_stats') }}
),

cleaned as (
    select
        player_stat_id,
        match_id,
        player_name,
        team,
        position,
        minutes_played,
        goals,
        assists,
        shots,
        shots_on_target,
        case when shots > 0 then round(100.0 * shots_on_target / shots, 2) else 0 end as shot_accuracy,
        passes_completed,
        pass_accuracy,
        tackles,
        yellow_cards,
        red_cards,
        -- Calculate goal involvement
        goals + assists as goal_involvement,
        current_timestamp() as loaded_at
    from source_data
)

select * from cleaned
