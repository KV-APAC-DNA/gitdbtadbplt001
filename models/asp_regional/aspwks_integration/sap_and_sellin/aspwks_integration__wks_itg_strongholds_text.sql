{{
    config(
        alias= "wks_itg_strongholds_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="view",
        tags=[""]
    )
}}

--import CTE

with sources as(
    SELECT *
    FROM {{ ref('aspitg_integration__stg_sdl_sap_bw_strongholds_text') }}
),
--logical CTE
final as(
    SELECT zstrong,
       langu,
       txtsh,
       txtmd,
       CRT_DTTM,
       UPDT_DTTM
  from {{ ref('aspitg_integration__stg_sdl_sap_bw_strongholds_text') }}
)
--final select
select * from final