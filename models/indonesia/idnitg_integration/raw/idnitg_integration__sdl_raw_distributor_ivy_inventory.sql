{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_distributor_ivy_inventory') }}
),
final as
 (
    select  
        distributor_code::varchar(25) as distributor_code,
        warehouse_code::varchar(25)  as warehouse_code,
        product_code::varchar(25) as product_code,
        batch_code::varchar(50) as batch_code,
        batch_expiry_date::varchar(50) as batch_expiry_date,
        uom::varchar(15) as uom,
        qty::number(10,0) as qty,
        current_timestamp()::varchar(50) as cdl_dttm,
        run_id::number(14,0) as run_id,
        source_file_name::varchar(256) as source_file_name
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.cdl_dttm > (select max(cdl_dttm) from {{ this }}) 
 {% endif %}
     
 )
select * from final