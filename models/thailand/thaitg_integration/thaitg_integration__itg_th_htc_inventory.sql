{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','recdate','whcode','productcode']
    )
}}


with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_htc_inventory') }}
),
trans as (
select
  cast(trim(date) as timestamp_ntz(9)) as recdate,
  trim(company)::varchar(10) as distributorid,
  trim(whcode)::varchar(20) as whcode,
  trim(productcode)::varchar(25) as productcode,
  trim(qty)::number(19,6) as qty,
  trim(amount)::number(19,6) as amount,
  trim(lotnumber)::varchar(200) as batchno,
  cast(trim(expirydate) as timestamp_ntz(9)) as expirydate,
  null as run_id,
  current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
),

final as (
select
  recdate,
  distributorid,
  whcode,
  productcode,
  qty,
  amount,
  batchno,
  expirydate,
  run_id,
  crt_dttm
from trans

)

select * from final