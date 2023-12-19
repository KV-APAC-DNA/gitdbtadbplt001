{{
    config(
        alias= "wks_itg_sls_grp_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="table",
        tags=["daily"]
    )
}}

--import CTE

with sources as(
    SELECT *
    FROM {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_sales_group_text') }}
),
--logical CTE
final as(
    select 
        mandt,
        spras,
        vkgrp,
        bezei,
        CRT_DTTM,
        UPDT_DTTM
     from sources
)
--final select
select * from final