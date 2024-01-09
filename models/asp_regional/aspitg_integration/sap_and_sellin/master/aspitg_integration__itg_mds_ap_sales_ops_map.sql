{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
    )
}}

--Import CTE
with source as (
    select * from {{ source('snapaspitg_integration','itg_mds_ap_sales_ops_map') }}
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