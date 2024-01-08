{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
    )
}}

with source as (
    select * from {{ ref('aspwks_integration__wks_edw_list_price') }}
)


select * from source
