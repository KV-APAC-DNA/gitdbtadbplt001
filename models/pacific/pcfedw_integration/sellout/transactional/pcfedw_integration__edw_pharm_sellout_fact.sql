with itg_pharm_sellout_raw as
(
    select * from {{ ref('pcfitg_integration__itg_pharm_sellout_raw') }}
),
final as
(
select 
    store_probe_id::varchar(10) as store_probe_id,
	store_name::varchar(30) as store_name,
	product_probe_id::varchar(10) as product_probe_id,
	product_description::varchar(60) as product_description,
	time_period::varchar(8) as time_period,
	units::number(18,0) as units,
	src_amt_value::number(18,6) as src_amt_value,
	amount::number(18,6) as amount,
	derived_price::number(12,6) as derived_price,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
from itg_pharm_sellout_raw
)
select * from final