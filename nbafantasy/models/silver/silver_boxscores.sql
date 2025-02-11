{{ config(
    materialized='incremental',
    unique_key='boxscore_id',
    incremental_strategy='merge',
    on_schema_change='fail'
)}}

with source as (
    select id
    from {{ source ('dbfantasy', 'boxscores')}}
    {% if is_incremental() %}
    EXCEPT
    select boxscore_id
    from {{ this }}
    {% endif %}
),

-- main query
final as (
    select
        id
        , week
        , match_header
        , home_team
        , home_wins
        , home_losses
        , home_ties
        , home_pts
        , home_pts_output
        , home_blk
        , home_blk_output
        , home_3pm
        , home_3pm_output
        , home_stl
        , home_stl_output
        , home_ast
        , home_ast_output
        , home_reb
        , home_reb_output
        , home_to
        , home_to_output
        , away_team
        , away_wins
        , away_losses
        , away_ties
        , away_pts
        , away_pts_output
        , away_blk
        , away_blk_output
        , away_3pm
        , away_3pm_output
        , away_stl
        , away_stl_output
        , away_ast
        , away_ast_output
        , away_reb
        , away_reb_output
        , away_to
        , away_to_output
        , match_output
    from 
        {{ source ('dbfantasy', 'boxscores')}}
    where id in (select id from source)
),

-- rename
renamed AS (
    select
        id AS boxscore_id
        , CAST(week AS bigint) AS match_week
        , match_header
        , home_team
        , CAST(home_wins AS bigint) AS home_wins
        , CAST(home_losses AS bigint) AS home_losses
        , CAST(home_ties AS bigint) AS home_ties
        , CAST(home_pts AS float) AS home_pts
        , home_pts_output
        , CAST(home_blk AS float) AS home_blk
        , home_blk_output
        , CAST(home_3pm AS float) AS home_3pm
        , home_3pm_output
        , CAST(home_stl AS float) AS home_stl
        , home_stl_output
        , CAST(home_ast AS float) AS home_ast
        , home_ast_output
        , CAST(home_reb AS float) AS home_reb
        , home_reb_output
        , CAST(home_to AS float) AS home_to
        , home_to_output
        , away_team
        , CAST(away_wins AS bigint) AS away_wins
        , CAST(away_losses AS bigint) AS away_losses
        , CAST(away_ties AS bigint) AS away_ties
        , CAST(away_pts AS float) AS away_pts
        , away_pts_output
        , CAST(away_blk AS float) AS away_blk
        , away_blk_output
        , CAST(away_3pm AS float) AS away_3pm
        , away_3pm_output
        , CAST(away_stl AS float) AS away_stl
        , away_stl_output
        , CAST(away_ast AS float) AS away_ast
        , away_ast_output
        , CAST(away_reb AS float) AS away_reb
        , away_reb_output
        , CAST(away_to AS float) AS away_to           
        , away_to_output
        , match_output
    from
        final
)

select * from renamed