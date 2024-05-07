with sdl_mds_ph_npi_peg_item as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_npi_peg_item') }}
),
final as (
SELECT 
code::varchar(50) as npi_item_cd,
peg_itemcode_code::varchar(50) as peg_item_cd,
peg_itemcode_name::varchar(255) as peg_item_desc,
salescycle::varchar(20) as sales_cycle,
current_timestamp()::timestamp_ntz(9) as crtd_dttm,
null::timestamp_ntz(9) as updt_dttm,

from sdl_mds_ph_npi_peg_item
)
select * from final