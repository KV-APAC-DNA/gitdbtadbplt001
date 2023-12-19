{{
    config(
        alias= "itg_sls_off_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['mandt','spras','vkbur'],
        merge_exclude_columns=["CRT_DTTM"],
        tags= ["daily"]
    )
}}

--import CTE

with sources as (
    select mandt as clnt,
            spras as lang_key,
            vkbur as sls_off,
            bezei as de,
            current_timestamp()::timestamp_ntz(9) as CRT_DTTM,
            current_timestamp()::timestamp_ntz(9) as UPDT_DTTM
        FROM {{ ref('aspwks_integration__wks_itg_sls_off_text') }}
),

--Logical CTE

final as(
    select * from sources
)
--final select
select * from final