{{
    config(
        materialized='table',
        tags=['analytics', 'weekly', 'premier_league', 'player']
    )
}}

with player_stats as (
    select * from {{ ref('stg_premier_league_player_stats') }}
),

matches as (
    select match_id, week from {{ ref('stg_premier_league_matches') }}
),

weekly_aggregates as (
    select
        m.week,
        ps.player_name,
        ps.team,
        ps.position,
        count(distinct ps.match_id) as appearances,
        sum(ps.minutes_played) as minutes,
        sum(ps.goals) as goals,
        sum(ps.assists) as assists,
        sum(ps.shots) as shots,
        sum(ps.shots_on_target) as shots_on_target,
        sum(ps.passes_completed) as passes_completed,
        avg(ps.pass_accuracy) as avg_pass_accuracy,
        sum(ps.tackles) as tackles,
        sum(ps.yellow_cards) as yellow_cards,
        sum(ps.red_cards) as red_cards
    from player_stats ps
    join matches m on ps.match_id = m.match_id
    group by m.week, ps.player_name, ps.team, ps.position
),

form_metrics as (
    select
        *,
        -- Performance score (weighted metric)
        round(
            (goals * 10) +
            (assists * 7) +
            (shots_on_target * 2) +
            (tackles * 1.5) +
            (avg_pass_accuracy * 0.5) -
            (yellow_cards * 5) -
            (red_cards * 20),
        2) as performance_score,
        -- Form rating out of 10
        round(
            least(10, greatest(0,
                5 + (goals * 1.5) + (assists * 1) + (shots_on_target * 0.2) - (yellow_cards * 0.5)
            )),
        1) as form_rating
    from weekly_aggregates
)

select
    week,
    player_name,
    team,
    position,
    appearances,
    minutes,
    goals,
    assists,
    round(cast(goals + assists as float) / nullif(appearances, 0), 2) as contribution_per_game,
    shots,
    shots_on_target,
    round(100.0 * shots_on_target / nullif(shots, 0), 1) as shot_accuracy_pct,
    round(passes_completed / nullif(appearances, 0), 1) as passes_per_game,
    round(avg_pass_accuracy, 1) as pass_accuracy,
    tackles,
    yellow_cards,
    red_cards,
    performance_score,
    form_rating,
    case
        when form_rating >= 8.0 then 'Excellent'
        when form_rating >= 7.0 then 'Very Good'
        when form_rating >= 6.0 then 'Good'
        when form_rating >= 5.0 then 'Average'
        else 'Below Average'
    end as form_category
from form_metrics
order by week desc, performance_score desc
