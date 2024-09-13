{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}
--Import CTE
with source as (
    select *
    from {{ source('sgpsdl_raw', 'sdl_sg_tp_closed_year_bal') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_sg_tp_closed_year_bal__null_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_sg_tp_closed_year_bal__lookup_test') }}
    )
),

--Logical CTE

--Final CTE
final as (
    select *
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_dt > (select max(curr_dt) from {{ this }}) 
 {% endif %}
)

--Final select
select * from final
