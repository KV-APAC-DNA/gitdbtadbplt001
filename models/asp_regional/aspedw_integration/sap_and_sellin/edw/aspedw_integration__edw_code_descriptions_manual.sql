{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

--Import CTE
with source as (
    select * from {{ source('snapaspedw_integration', 'edw_code_descriptions_manual') }}
),


--Logical CTE

-- Final CTE
final as (
    select
  * from source)


--Final select
select * from final 