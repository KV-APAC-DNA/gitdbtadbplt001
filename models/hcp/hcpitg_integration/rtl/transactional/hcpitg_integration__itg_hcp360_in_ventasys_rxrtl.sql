{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE TO_CHAR(dcr_dt,'YYYYMM') IN (SELECT  TO_CHAR(sdl.dcr_dt,'YYYYMM') 
                                    FROM  {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rxrtl') }} sdl
                                  where sdl.filename not in (
                                  select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxrtl__null_test') }}
                                  union all
                                  select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxrtl__duplicate_test') }}
                                  )
                                  )
        {% endif %}"
    )
}}
with sdl_hcp360_in_ventasys_rxrtl as (
    select *,dense_rank() over (partition by dcr_dt order by filename desc ) as rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rxrtl') }} 
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxrtl__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxrtl__duplicate_test') }}
    ) qualify rn=1
),
final as 
(
  select
    team_name::varchar(20) as team_name,
    v_rxid::varchar(50) as v_rxid,
    v_empid::varchar(50) as v_empid,
    v_custid_dr::varchar(50) as v_custid_dr,
    dcr_dt::date as dcr_dt,
    rx_product::varchar(200) as rx_product,
    rx_units::number(38,2) as rx_units,
    v_custid_rtl::varchar(50) as v_custid_rtl,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename
  from sdl_hcp360_in_ventasys_rxrtl
)
select * from final