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
  sls_org,
  material,
  cond_rec_no,
  matl_grp,
  valid_to,
  knart,
  dt_from,
  CAST(amount as decimal(20, 4)) as amount,
  currency,
  unit,
  record_mode,
  comp_cd,
  price_unit,
  zcurrfpa,
  CAST(cdl_dttm AS TEXT) as cdl_dttm,
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

