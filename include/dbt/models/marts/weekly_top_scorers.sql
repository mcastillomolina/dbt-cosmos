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

weekly_player_stats as (
    select
        m.week,
        ps.player_name,
        ps.team,
        ps.position,
        count(distinct ps.match_id) as matches_played,
        sum(ps.minutes_played) as total_minutes,
        sum(ps.goals) as goals,
        sum(ps.assists) as assists,
        sum(ps.goal_involvement) as goal_involvements,
        sum(ps.shots) as shots,
        sum(ps.shots_on_target) as shots_on_target,
        round(avg(ps.shot_accuracy), 2) as avg_shot_accuracy,
        round(avg(ps.pass_accuracy), 2) as avg_pass_accuracy
    from player_stats ps
    join matches m on ps.match_id = m.match_id
    group by m.week, ps.player_name, ps.team, ps.position
)

select
    week,
    player_name,
    team,
    position,
    matches_played,
    total_minutes,
    goals,
    assists,
    goal_involvements,
    round(cast(goals as float) / nullif(matches_played, 0), 2) as goals_per_game,
    round(cast(goal_involvements as float) / nullif(matches_played, 0), 2) as involvements_per_game,
    shots,
    shots_on_target,
    avg_shot_accuracy,
    avg_pass_accuracy
from weekly_player_stats
where goals > 0  -- Only players who scored
order by week desc, goals desc, assists desc
