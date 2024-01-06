{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
    )
}}

--Import CTE
with 
source as (
   select * from {{ source('snapaspedw_integration', 'edw_plant_dim') }}
),
--Logical CTE

-- Final CTE
final as (
select
  *
  --current_timestamp()::timestamp_ntz(9) as crt_dttm,
--   current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)


--Final select
select * from final 