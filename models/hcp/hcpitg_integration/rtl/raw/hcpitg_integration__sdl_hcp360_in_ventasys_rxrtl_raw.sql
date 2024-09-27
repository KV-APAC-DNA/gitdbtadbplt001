{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rxrtl')}}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxrtl__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxrtl__duplicate_test') }}
    )
),
final as
 (
    select
    team_name,
	v_rxid,
	v_empid,
	v_custid_dr,
	dcr_dt,
	rx_product,
	rx_units,
	v_custid_rtl,
	crt_dttm,
	filename         
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final