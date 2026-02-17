{{
    config(
        materialized='table',
        tags=['analytics', 'weekly', 'premier_league', 'team']
    )
}}

with matches as (
    select * from {{ ref('stg_premier_league_matches') }}
),

home_stats as (
    select
        week,
        home_team as team,
        count(*) as home_matches,
        sum(case when home_score > away_score then 1 else 0 end) as home_wins,
        sum(case when home_score < away_score then 1 else 0 end) as home_losses,
        sum(case when home_score = away_score then 1 else 0 end) as home_draws,
        sum(home_score) as home_goals_scored,
        sum(away_score) as home_goals_conceded
    from matches
    group by week, home_team
),

away_stats as (
    select
        week,
        away_team as team,
        count(*) as away_matches,
        sum(case when away_score > home_score then 1 else 0 end) as away_wins,
        sum(case when away_score < home_score then 1 else 0 end) as away_losses,
        sum(case when away_score = home_score then 1 else 0 end) as away_draws,
        sum(away_score) as away_goals_scored,
        sum(home_score) as away_goals_conceded
    from matches
    group by week, away_team
),

weekly_summary as (
    select
        coalesce(h.week, a.week) as week,
        coalesce(h.team, a.team) as team,
        coalesce(h.home_matches, 0) + coalesce(a.away_matches, 0) as total_matches,
        coalesce(h.home_wins, 0) + coalesce(a.away_wins, 0) as wins,
        coalesce(h.home_losses, 0) + coalesce(a.away_losses, 0) as losses,
        coalesce(h.home_draws, 0) + coalesce(a.away_draws, 0) as draws,
        coalesce(h.home_goals_scored, 0) + coalesce(a.away_goals_scored, 0) as goals_scored,
        coalesce(h.home_goals_conceded, 0) + coalesce(a.away_goals_conceded, 0) as goals_conceded,
        (coalesce(h.home_goals_scored, 0) + coalesce(a.away_goals_scored, 0)) -
        (coalesce(h.home_goals_conceded, 0) + coalesce(a.away_goals_conceded, 0)) as goal_difference,
        (coalesce(h.home_wins, 0) + coalesce(a.away_wins, 0)) * 3 +
        (coalesce(h.home_draws, 0) + coalesce(a.away_draws, 0)) as points
    from home_stats h
    full outer join away_stats a
        on h.week = a.week and h.team = a.team
)

select
    week,
    team,
    total_matches,
    wins,
    draws,
    losses,
    round(100.0 * wins / nullif(total_matches, 0), 2) as win_percentage,
    goals_scored,
    goals_conceded,
    goal_difference,
    points,
    round(cast(points as float) / nullif(total_matches, 0), 2) as points_per_game
from weekly_summary
order by week desc, points desc, goal_difference desc
