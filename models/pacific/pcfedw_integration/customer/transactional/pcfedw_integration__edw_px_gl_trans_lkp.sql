with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_px_gl_ciw_mapping') }}
),
final as
(
select
    code::number(18,0) as row_id,
    name::varchar(40) as gl_trans,
    sap_account::varchar(40) as sap_account,
	promax_measure::varchar(40) as promax_measure,
	promax_bucket::varchar(40) as promax_bucket,
    null::varchar(255) as tp_category,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from final