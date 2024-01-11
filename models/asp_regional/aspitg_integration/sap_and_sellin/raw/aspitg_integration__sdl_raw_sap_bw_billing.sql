{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="append",
        unique_key=["file_name"]
    )}}

with source as (
     select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_billing') }} ),
final as (
    select * from source
)

select * from final