{{
    config(
        alias= "wks_itg_sls_off_text",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="view",
        tags=[""]
    )
}}

--import CTE

with sources as(
    SELECT *
    FROM {{ ref('aspitg_integration__stg_sdl_sap_ecc_sales_office_text') }}
),
--logical CTE
final as(
    select 
        mandt,
        spras,
        vkbur,
        bezei,
        CRT_DTTM,
        UPDT_DTTM
     from sources
)
--final select
select * from final