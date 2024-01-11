{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )}}

with source as (
     select * from {{ ref('aspitg_integration__vw_stg_sdl_account_attr_ciw') }} ),
final as (
    select * from source
    where
    filename = '/File_Name'
)

select * from final