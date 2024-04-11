with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ciw_adjustment') }}
),
final as
(
select 
    name::varchar(10) as time_period,
	customer_id::varchar(10) as cust_id,
	prod_mjr_cd::varchar(30) as prod_mjr_cd,
	sap_accnt::varchar(30) as sap_accnt,
	amt_obj_crncy::number(18,2) as amt_obj_crncy,
	local_ccy::varchar(10) as local_ccy,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from final