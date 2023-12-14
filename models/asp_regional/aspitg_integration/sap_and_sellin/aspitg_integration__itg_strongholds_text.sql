{{
    config(
        alias= "itg_strongholds_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['zstrong'],
        merge_exclude_columns=["CRT_DTTM"],
        tags= [""]
    )
}}

--import CTE

with sources as (
   SELECT zstrong as strongholds,
       langu as language_key,
       txtsh as short_desc,
       txtmd as medium_desc,
            current_timestamp() as CRT_DTTM,
            current_timestamp() as UPDT_DTTM
        FROM {{ ref('aspwks_integration__wks_itg_strongholds_text') }}
),

--Logical CTE

final as(
    select * from sources
)
--final select
select * from final