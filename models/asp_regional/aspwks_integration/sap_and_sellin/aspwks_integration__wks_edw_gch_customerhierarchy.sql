{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}


--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_gcch_cust_hier') }}
),

--Logical CTE

--Final CTE
final as (
    select
        *,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
  where
    not customer is null
)

--Final select
select * from final