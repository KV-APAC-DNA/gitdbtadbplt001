{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','recdate','whcode','productcode']
    )
}}

with sdl_th_dms_inventory_fact as (
    select * from {{ source('thasdl_raw', 'sdl_th_dms_inventory_fact') }}
),
final as (
select
  to_date(replace(recdate,'/',''),'YYYYMMDD')::timestamp_ntz(9) as recdate,
  distributorid::varchar(10) as distributorid,
  whcode::varchar(20) as whcode,
  productcode::varchar(25) as productcode,
  qty::number(19,6) as qty,
  amount::number(19,6) as amount,
  batchno::varchar(200) as batchno,
  to_date(expirydate::varchar,'YYYYMMDD')::timestamp_ntz(9) as expirydate,
  current_timestamp()::date as curr_date,
  run_id::number(18,0) as run_id
from sdl_th_dms_inventory_fact)
select * from final
