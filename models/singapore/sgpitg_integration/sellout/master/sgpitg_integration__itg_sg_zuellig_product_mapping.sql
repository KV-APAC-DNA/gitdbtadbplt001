
with source as (
    select * from {{ source('sgpsdl_raw', 'sdl_sg_zuellig_product_mapping') }}
),
final as 
(
select
  zp_material::varchar(20) as zp_material,
  zp_item_code::varchar(20) as zp_item_code,
  jj_code::varchar(20) as jj_code,
  item_name::varchar(255) as item_name,
  brand::varchar(20) as brand,
  cdl_dttm::varchar(20) as cdl_dttm,
  curr_dt::timestamp_ntz(9)as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name::varchar(255) as file_name,
  run_id::number(14,0) as run_id
from source
)
select * from final
