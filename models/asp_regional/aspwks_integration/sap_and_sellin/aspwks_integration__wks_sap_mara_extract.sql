{{
    config(
        alias= "wks_sap_mara_extract",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags= ["daily"]
    )
}}


--Import CTE
with source as (
    select * from {{ ref('a_exception_aspitg_integration__vw_stg_sdl_sap_mara_extract') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source 
)

--Final select 
select * from final