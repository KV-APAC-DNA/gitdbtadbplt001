{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["prod_hier","langu"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_prod_hier') }}
    
),

--Logical CTE

--Final CTE
final as (
    select
    prod_hier::varchar(18) as prod_hier,
    langu::varchar(1) as langu,
    txtmd::varchar(40) as txtmd,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final