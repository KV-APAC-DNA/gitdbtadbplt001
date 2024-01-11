{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_price_list') }}
),
final as(
  select
  sls_org::VARCHAR(50) as sls_org,
  material::VARCHAR(50) as material,
  cond_rec_no::VARCHAR(100) as cond_rec_no,
  matl_grp::VARCHAR(100) as matl_grp,
  valid_to::VARCHAR(20) as valid_to,
  knart::VARCHAR(20) as knart,
  dt_from::VARCHAR(20) as dt_from,
  CAST(amount as decimal(20, 4)) as amount,
  currency::VARCHAR(20) as currency,
  unit::VARCHAR(20) as unit,
  record_mode::VARCHAR(100) as record_mode,
  comp_cd::VARCHAR(100) as comp_cd,
  price_unit::VARCHAR(50) as price_unit,
  zcurrfpa::VARCHAR(20) as zcurrfpa,
  cdl_dttm::VARCHAR(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm, 
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  null as file_name
  from source
  {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    left join {{this}} as itg
  on trim(sdl.sls_org) = trim(itg.sls_org)
  and ltrim(sdl.material, 0) = ltrim(itg.material, 0)
  and itg.cond_rec_no = itg.cond_rec_no
  and to_date(sdl.valid_to, 'YYYYMMDD') = to_date(itg.valid_to, 'YYYYMMDD')
  and to_date(sdl.dt_from, 'YYYYMMDD') = to_date(itg.dt_from, 'YYYYMMDD')
  and sdl.amount = itg.amount
where
  itg.material is null 
  {% endif %}

)

select * from final

