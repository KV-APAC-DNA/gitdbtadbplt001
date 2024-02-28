with sdl_th_dms_inventory_fact as (
    select * from {{ source('thasdl_raw', 'sdl_th_dms_inventory_fact') }}
),
final as (
select
  recdate
  distributorid,
  whcode,
  productcode,
  cast(qty as decimal(19, 6)),
  cast(amount as decimal(19, 6)),
  batchno,
  to_date(expirydate::varchar),
  convert_timezone('Asia/Singapore', current_timestamp()) as curr_date,
  run_id
from sdl_th_dms_inventory_fact)
select * from final