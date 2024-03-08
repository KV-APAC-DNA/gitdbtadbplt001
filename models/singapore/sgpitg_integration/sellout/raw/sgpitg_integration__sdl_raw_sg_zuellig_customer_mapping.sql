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
    from {{ source('sgpsdl_raw', 'sdl_sg_zuellig_customer_mapping') }}
),
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