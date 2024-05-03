with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_retailer_soldto_map') }}
),
final as
(
select
    upper(trim(retailer_name))::varchar(100) as retailer_name,
    trim(sold_to)::varchar(20) as sold_to,
    current_timestamp::timestamp_ntz(9) as crt_dttm
from source
)
select * from final