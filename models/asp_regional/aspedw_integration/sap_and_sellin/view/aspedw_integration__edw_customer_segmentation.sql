{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "view"
    )
}}


with source as (
    select * from {{ ref('aspedw_integration__v_rpt_customer_segmentation') }}
),



final as (
  select 
  *,
  current_timestamp()::timestamp_ntz(9) as crt_dttm
  from source
)


--Final select
select * from final 