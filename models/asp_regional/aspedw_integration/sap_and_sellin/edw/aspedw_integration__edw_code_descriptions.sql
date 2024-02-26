{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["source_type","code_type","code"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_code_descriptions') }}
    
),

--Logical CTE

--Final CTE
final as (
    select
        source_type::varchar(10) as source_type,
        code_type::varchar(50) as code_type,
        code::varchar(15) as code,
        code_desc::varchar(50) as code_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

--Final select
select * from final