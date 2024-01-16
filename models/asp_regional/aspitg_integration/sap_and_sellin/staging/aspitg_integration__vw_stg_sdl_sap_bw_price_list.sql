
with source as(
    select * from {{ source('bwa_access', 'bwa_list_price') }}
),
final as(
 SELECT
  salesorg as sls_org,
  material as material,
  condrecno as cond_rec_no,
  matl_group as matl_grp,
  validto as valid_to,
  knart as knart,
  datefrom as dt_from,
  amount as amount,
  currency as currency,
  unit as unit,
  recordmode as record_mode,
  comp_code as comp_cd,
  price_unit as price_unit,
  bic_zcurrfpa as zcurrfpa,
  _ingestiontimestamp_ as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  NULL as file_name
  FROM source

)
select * from final