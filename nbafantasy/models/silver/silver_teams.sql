-- import

with source as (
    select 
        "Team Name"
        , "Owner"
        , "Team ID"
        , "Abbreviation"
        , "Division ID"
        , "Division Name"
        , "Total Wins"
        , "Total Losses"
        , "Standing"
    from 
        {{ source ('dbfantasy', 'teams')}}
),
-- renamed

renamed AS (
    select
        CAST("Team ID" AS bigint) AS "Team ID"
        , "Team Name"
        , "Abbreviation" AS "Team Abbreviation"
        , "Owner" AS "Team Owner"
        , CAST("Division ID" AS bigint) AS "Division ID"
        , "Division Name"
        , CAST("Standing" AS bigint) AS "Current Standing"
        , CAST("Total Wins" AS bigint) AS "Team Wins"
        , CAST("Total Losses" AS bigint) AS "Team Losses"
    from 
        source
)

select * from renamed