with source as (
    select * from {{ source('myssdl_raw','sdl_mds_my_gt_outletattributes') }}
),
transformed as (
select
  distributor_id as cust_id,
  distributor_name as cust_nm,
  customer_code as outlet_id,
  customer_name1 as outlet_desc,
  outlet_type1 as outlet_type1,
  outlet_type2 as outlet_type2,
  outlet_type3 as outlet_type3,
  outlet_type4 as outlet_type4,
  town as town,
  cust_year as cust_year,
  salesman_code as slsmn_cd,
  null as cdl_dttm,
  current_timestamp() as crtd_dttm,
  null as updt_dttm
from source
),
final as (
select
	cust_id::varchar(20) as cust_id,
	cust_nm::varchar(100) as cust_nm,
	outlet_id::varchar(25) as outlet_id,
	outlet_desc::varchar(100) as outlet_desc,
	outlet_type1::varchar(50) as outlet_type1,
	outlet_type2::varchar(50) as outlet_type2,
	outlet_type3::varchar(100) as outlet_type3,
	outlet_type4::varchar(100) as outlet_type4,
	town::varchar(50) as town,
	cust_year::varchar(50) as cust_year,
	slsmn_cd::varchar(30) as slsmn_cd,
	cdl_dttm::varchar(50) as cdl_dttm,
	crtd_dttm::timestamp_ntz(9) as crtd_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final