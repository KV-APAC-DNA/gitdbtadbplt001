with source as (
    select * from {{ source('pcfsdl_raw','sdl_mds_pacific_terms_master') }}
),
final as
(
select 
    country::varchar(100) as country,
	code::varchar(50) as sls_grp_cd,
	name::varchar(255) as sls_grp_nm,
	terms_percentage::number(20,4) as terms_percentage,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm
from source
)
select * from final