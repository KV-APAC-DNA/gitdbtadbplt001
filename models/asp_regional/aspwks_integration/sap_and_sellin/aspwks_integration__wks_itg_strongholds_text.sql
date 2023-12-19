{{
    config(
        alias= "wks_itg_strongholds_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="table",
        tags=["daily"]
    )
}}

--import CTE

with sources as(
    SELECT *
    FROM {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_strongholds_text') }}
),
--logical CTE
final as(
    SELECT zstrong,
       langu,
       txtsh,
       txtmd,
       CRT_DTTM,
       UPDT_DTTM
 from sources
)
--final select
select * from final