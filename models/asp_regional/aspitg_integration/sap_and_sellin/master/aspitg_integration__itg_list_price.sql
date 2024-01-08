{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
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
  CAST(amount AS DECIMAL(20, 4)) AS amount,
  currency,
  unit,
  record_mode,
  comp_cd,
  price_unit,
  zcurrfpa,
  cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm, 
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  null as file_name
  from source

)

select * from final

