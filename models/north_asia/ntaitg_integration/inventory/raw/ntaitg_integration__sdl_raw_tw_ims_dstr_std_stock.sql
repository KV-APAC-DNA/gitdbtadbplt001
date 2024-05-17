{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_stock') }}
),
final as
(
    select 
        distributor_code as distributor_code,
        ean as ean,
        distributor_product_code as distributor_product_code,
        quantity as quantity,
        total_cost as total_cost,
        inventory_date as inventory_date,
        distributors_product_name as distributors_product_name,
        uom as uom,
        storage_name as storage_name,
        crt_dttm as crt_dttm,
        updt_dttm as upd_dttm,
        filename as filename,
        null as run_id
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
