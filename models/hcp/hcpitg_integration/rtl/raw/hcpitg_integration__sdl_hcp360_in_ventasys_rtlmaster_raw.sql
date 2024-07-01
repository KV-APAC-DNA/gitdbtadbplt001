{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rtlmaster')}}
),
final as
 (
    SELECT 
        team_name,
        v_custid_rtl,
        v_terrid,
        cust_name,
        cust_type,
        is_active,
        cust_endtered_date,
        urc,
        crt_dttm,
        filename
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final