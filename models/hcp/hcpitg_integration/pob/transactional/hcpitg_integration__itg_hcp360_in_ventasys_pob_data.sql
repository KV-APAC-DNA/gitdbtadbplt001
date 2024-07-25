{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE TO_CHAR(dcr_dt,'YYYYMM') IN (SELECT  TO_CHAR(sdl.dcr_dt,'YYYYMM') 
                                   FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_pob_data')}} sdl)
        {% endif %}"
    )
}}

with sdl_hcp360_in_ventasys_pob_data as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_pob_data') }}
),
final as(
select 
team_name::varchar(20) as team_name,
v_pobid::varchar(50) as v_pobid,
v_empid::varchar(50) as v_empid,
v_custid_rtl::varchar(50) as v_custid_rtl,
dcr_dt::date as dcr_dt,
pob_product::varchar(200) as pob_product,
pob_units::number(38,2) as pob_units,
convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
filename::varchar(50) as filename
FROM
    sdl_hcp360_in_ventasys_pob_data
)
select * from final

