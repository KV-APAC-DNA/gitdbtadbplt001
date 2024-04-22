with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_cust_attrb') }}
),
final as
(
select 
    sls_org::varchar(20) as sls_org,
	cmp_id::varchar(20) as cmp_id,
	market::varchar(20) as country,
	dstr_chnl::varchar(20) as dstr_chnl,
	chnl_cd::varchar(20) as chnl_cd,
	chnl_desc::varchar(20) as chnl_desc,
	sls_ofc::varchar(20) as sls_ofc,
	sls_ofc_desc::varchar(30) as sls_ofc_desc,
	sls_grp::varchar(20) as sls_grp,
	sls_grp_desc::varchar(30) as sls_grp_desc,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from final