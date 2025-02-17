{{ config(
    materialized='incremental',
    unique_key='activity_id',
    incremental_strategy='merge',
    on_schema_change='fail'
)}}

with source as (
    select id
    from {{ source ('dbfantasy', 'last_activities')}}
    {% if is_incremental() %}
    EXCEPT
    select activity_id
    from {{ this }}
    {% endif %}
),

final AS (
    select
        id
        , processed_at
        , team
        , type
        , player_name
        , notes
    from 
        {{ source ('dbfantasy', 'last_activities')}}
    where id in (select id from source)
),

renamed AS (
    select 
        id AS activity_id
        , processed_at
        , team as fantasy_team
        , type as activity_type
        , player_name
        , notes 
    from 
        final
)

select * from renamed