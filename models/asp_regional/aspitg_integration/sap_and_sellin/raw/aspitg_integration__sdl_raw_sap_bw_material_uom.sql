{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_material_uom') }}
),
final as(
    select         
        material,
        unit,
        base_uom,
        recordmode as record_mode,
        uomz1d,
        uomn1d,
        file_name,
        cdl_dttm,
        crt_dttm as curr_dt 
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(curr_dt) from {{ this }}) 
 {% endif %}
)

select * from final