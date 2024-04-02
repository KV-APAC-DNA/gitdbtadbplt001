with edw_vw_vn_mt_offtake_exception_report as 
(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_offtake_exception_report') }}
),
final as 
(
select
    account::varchar(13) as account,
	month_id::varchar(23) as month_id,
	product_cd::varchar(100) as product_cd,
	customer_cd::varchar(200) as customer_cd,
	store_name::varchar(255) as store_name,
	barcode::varchar(200) as barcode,
	product_name::varchar(1125) as product_name,
	franchise::varchar(1687) as franchise,
	category::varchar(1687) as category,
	sub_brand::varchar(1350) as sub_brand,
	sub_category::varchar(1687) as sub_category,
	amount::number(38,9) as amount,
	amount_usd::number(38,14) as amount_usd
from 
edw_vw_vn_mt_offtake_exception_report
)
select * from final