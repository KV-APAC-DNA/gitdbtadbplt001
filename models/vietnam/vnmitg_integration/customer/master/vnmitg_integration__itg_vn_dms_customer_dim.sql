{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['outlet_id']
    )
}}

with source as (
    select *, dense_rank() over (partition by outlet_id order by file_name desc) rnk 
    from {{ source('vnmsdl_raw','sdl_vn_dms_customer_dim') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_customer_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_customer_dim__duplicate_test')}}
    ) qualify rnk = 1
),

final as
(
select
    sap_id::varchar(30) as sap_id,
	dstrbtr_id::varchar(30) as dstrbtr_id,
	cntry_code::varchar(10) as cntry_code,
	outlet_id::varchar(30) as outlet_id,
	outlet_name::varchar(500) as outlet_name,
	address_1::varchar(200) as address_1,
	address_2::varchar(200) as address_2,
	telephone::varchar(100) as telephone,
	fax::varchar(20) as fax,
	city::varchar(100) as city,
	postcode::varchar(20) as postcode,
	region::varchar(20) as region,
	channel_group::varchar(2) as channel_group,
	sub_channel::varchar(20) as sub_channel,
	sales_route_id::varchar(30) as sales_route_id,
	sales_route_name::varchar(100) as sales_route_name,
	sales_group::varchar(30) as sales_group,
	salesrep_id::varchar(30) as salesrep_id,
	salesrep_name::varchar(100) as salesrep_name,
	gps_lat::varchar(30) as gps_lat,
	gps_long::varchar(30) as gps_long,
	status::varchar(1) as status,
	district::varchar(100) as district,
	trim(province, ',')::varchar(100) as province,
    to_date(to_timestamp_ntz(crt_date,'mm/dd/yyyy hh12:mi:ss pm')) as crt_date,
    to_date(to_timestamp_ntz(date_off,'mm/dd/yyyy hh12:mi:ss pm')) as date_off,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    null::number(14,0) as run_id,
    shop_type::varchar(100) as  shop_type  ,
    file_name::varchar(255) as file_name
from source
)
select * from final
   