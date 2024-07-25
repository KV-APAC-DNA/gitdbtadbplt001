{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_pob_data')}}
),
final as
 (
    select
        team_name,
        v_pobid,
        v_empid,
        v_custid_rtl,
        dcr_dt,
        pob_product,
        pob_units,
        crt_dttm,
        filename,        
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final