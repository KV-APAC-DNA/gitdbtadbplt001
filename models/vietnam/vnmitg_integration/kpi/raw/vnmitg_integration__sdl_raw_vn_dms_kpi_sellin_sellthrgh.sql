{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}
--Import CTE
with source as 
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_kpi_sellin_sellthrgh') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi_sellin_sellthrgh__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi_sellin_sellthrgh__duplicate_test')}}
    )
),
--Final CTE
final as (
    select *
  from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_date > (select max(curr_date) from {{ this }}) 
 {% endif %}
)

--Final select
select * from final