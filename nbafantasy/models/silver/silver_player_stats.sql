{{ config(
    materialized='incremental',
    unique_key='player_id',
    incremental_strategy='merge',
    on_schema_change='fail'
)}}

with source as (
    select id
    from {{ source ('dbfantasy', 'player_stats')}}
    {% if is_incremental() %}
    EXCEPT
    select player_id
    from {{ this }}
    {% endif %}
),

recast AS (
    select
        id
        , name
        , "position"
        , "posRank"
        , "proTeam"
        , "status"
        , "team"
        , avg_stats::text as avg_stats_text
        , "7d_stats"::text as stats_7d_text
        , "15d_stats"::text as stats_15d_text
        , "30d_stats"::text as stats_30d_text
        , "injured"
    from 
        {{ source ('dbfantasy', 'player_stats')}}
    where id in (select id from source)

),

-- main query
final as (
    select
        id
        , "name"
        , "position"
        , "posRank"
        , "proTeam"
        , "status"
        , "team"
        , ARRAY[
                (regexp_match(avg_stats_text, 'PTS'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'BLK'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'STL'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'AST'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'REB'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'TO'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, '3PM'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'FG%'': ([0-9.]+)'))[1]::numeric,
                (regexp_match(avg_stats_text, 'FT%'': ([0-9.]+)'))[1]::numeric
        ] AS avg_stats_array
        , ARRAY[
                (regexp_match("stats_7d_text", 'PTS'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'BLK'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'STL'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'AST'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'REB'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'TO'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", '3PM'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'FG%'': ([0-9.]+)'))[1]::numeric,
                (regexp_match("stats_7d_text", 'FT%'': ([0-9.]+)'))[1]::numeric
        ] "7d_stats_array"
        , ARRAY[
            (regexp_match("stats_15d_text", 'PTS'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'BLK'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'STL'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'AST'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'REB'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'TO'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", '3PM'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'FG%'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_15d_text", 'FT%'': ([0-9.]+)'))[1]::numeric
        ] "15d_stats_array"
        , ARRAY[
            (regexp_match("stats_30d_text", 'PTS'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'BLK'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'STL'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'AST'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'REB'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'TO'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", '3PM'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'FG%'': ([0-9.]+)'))[1]::numeric,
            (regexp_match("stats_30d_text", 'FT%'': ([0-9.]+)'))[1]::numeric
        ] "30d_stats_array"
        , "injured"
    from 
        recast
),

-- rename
renamed AS (
    select
        id AS player_id
        , "name" AS player_name
        , "position"
        , "posRank" 
        , "proTeam"
        , "status"
        , "team" AS fantasy_team
        , avg_stats_array[1] AS avg_pts
        , avg_stats_array[2] AS avg_blk
        , avg_stats_array[3] AS avg_stl
        , avg_stats_array[4] AS avg_ast
        , avg_stats_array[5] AS avg_reb
        , avg_stats_array[6] AS avg_to
        , avg_stats_array[7] AS avg_3pm
        , avg_stats_array[8] AS avg_fg_pct
        , avg_stats_array[9] AS avg_ft_pct
        , "7d_stats_array"[1] AS "7d_avg_pts"
        , "7d_stats_array"[2] AS "7d_avg_blk"
        , "7d_stats_array"[3] AS "7d_avg_stl"
        , "7d_stats_array"[4] AS "7d_avg_ast"
        , "7d_stats_array"[5] AS "7d_avg_reb"
        , "7d_stats_array"[6] AS "7d_avg_to"
        , "7d_stats_array"[7] AS "7d_avg_3pm"
        , "7d_stats_array"[8] AS "7d_avg_fg_pct"
        , "7d_stats_array"[9] AS "7d_avg_ft_pct"
        , "15d_stats_array"[1] AS "15d_avg_pts"
        , "15d_stats_array"[2] AS "15d_avg_blk"
        , "15d_stats_array"[3] AS "15d_avg_stl"
        , "15d_stats_array"[4] AS "15d_avg_ast"
        , "15d_stats_array"[5] AS "15d_avg_reb"
        , "15d_stats_array"[6] AS "15d_avg_to"
        , "15d_stats_array"[7] AS "15d_avg_3pm"
        , "15d_stats_array"[8] AS "15d_avg_fg_pct"
        , "15d_stats_array"[9] AS "15d_avg_ft_pct"
        , "30d_stats_array"[1] AS "30d_avg_pts"
        , "30d_stats_array"[2] AS "30d_avg_blk"
        , "30d_stats_array"[3] AS "30d_avg_stl"
        , "30d_stats_array"[4] AS "30d_avg_ast"
        , "30d_stats_array"[5] AS "30d_avg_reb"
        , "30d_stats_array"[6] AS "30d_avg_to"
        , "30d_stats_array"[7] AS "30d_avg_3pm"
        , "30d_stats_array"[8] AS "30d_avg_fg_pct"
        , "30d_stats_array"[9] AS "30d_avg_ft_pct"
        , "injured"
        from
            final
)

select * from renamed