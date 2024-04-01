{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}
--Import CTE
with source as 
(
    select *
    from {{ source('vnmsdl_raw', 'sdl_vn_interface_cpg') }}
),
--Final CTE
final as (
    select *
  from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

--Final select
select * from final