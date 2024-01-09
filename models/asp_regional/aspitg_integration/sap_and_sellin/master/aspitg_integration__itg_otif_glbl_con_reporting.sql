{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
    )
}}

--Import CTE
with source as (
    select * from {{ source('snapaspitg_integration','itg_otif_glbl_con_reporting') }}
),

--Logical CTE

--Final CTE
final as (
    select
    *
    from source
)
   

--Final select
select * from final 