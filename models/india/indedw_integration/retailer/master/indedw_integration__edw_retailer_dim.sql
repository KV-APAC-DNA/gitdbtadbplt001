with source as 
(
    select * from {{ ref('indwks_integration__wks_retailermaster_upd') }}
),
temp_a as 
(
    select 
    retailer_code::varchar(50) as retailer_code,
	start_date::timestamp_ntz(9) as start_date,
	end_date::timestamp_ntz(9) as end_date,
	customer_code::varchar(50) as customer_code,
	customer_name::varchar(150) as customer_name,
	retailer_name::varchar(150) as retailer_name,
	retailer_address1::varchar(250) as retailer_address1,
	retailer_address2::varchar(250) as retailer_address2,
	retailer_address3::varchar(250) as retailer_address3,
	region_code::number(38,0) as region_code,
	region_name::varchar(50) as region_name,
	zone_code::number(38,0) as zone_code,
	zone_name::varchar(50) as zone_name,
	zone_classification::varchar(50) as zone_classification,
	territory_code::number(38,0) as territory_code,
	territory_name::varchar(50) as territory_name,
	territory_classification::varchar(50) as territory_classification,
	state_code::number(38,0) as state_code,
	state_name::varchar(50) as state_name,
	town_name::varchar(50) as town_name,
	town_classification::varchar(50) as town_classification,
	class_code::varchar(50) as class_code,
	class_desc::varchar(50) as class_desc,
	outlet_type::varchar(50) as outlet_type,
	channel_code::varchar(50) as channel_code,
	channel_name::varchar(150) as channel_name,
	business_channel::varchar(50) as business_channel,
	loyalty_desc::varchar(50) as loyalty_desc,
	registration_date::number(18,0) as registration_date,
	status_cd::varchar(50) as status_cd,
	status_desc::varchar(10) as status_desc,
	csrtrcode::varchar(50) as csrtrcode,
	convert_timezone('UTC' , current_timestamp())::timestamp_ntz(9) as crt_dttm, 
    convert_timezone('UTC' , current_timestamp())::timestamp_ntz(9) as updt_dttm,
	actv_flg::varchar(1) as actv_flg,
	retailer_category_cd::varchar(25) as retailer_category_cd,
	retailer_category_name::varchar(50) as retailer_category_name,
	rtrlatitude::varchar(40) as rtrlatitude,
	rtrlongitude::varchar(40) as rtrlongitude,
	rtruniquecode::varchar(100) as rtruniquecode,
	createddate::timestamp_ntz(9) as createddate,
	to_date(file_rec_dt) as file_rec_dt,
	type_name::varchar(50) as type_name,
	town_code::varchar(50) as town_code
    from source
),
temp_b as 
(
    select 
    '-1' as retailer_code, 
    convert_timezone('UTC' , current_timestamp())::timestamp_ntz(9) as start_date, 
    TO_DATE('99991231','YYYYMMDD')::timestamp_ntz(9) as end_date, 
    '-1' as customer_code, 
    'Unknown' as customer_name, 
    'Unknown' as retailer_name, 
    'Unknown' as retailer_address1, 
    'Unknown' as retailer_address2, 
    'Unknown' as retailer_address3, 
    '-1'as region_code, 
    'Unknown' as region_name, 
    '-1'as zone_code, 
    'Unknown' as zone_name, 
    'Unknown' as zone_classification, 
    '-1'as territory_code, 
    'Unknown' as territory_name, 
    'Unknown' as territory_classification, 
    '-1'as state_code, 
    'Unknown' as state_name, 
    -- '-1'as town_code, 
    'Unknown' as town_name, 
    'Unknown' as town_classification, 
    'Unknown' as class_code, 
    'Unknown' as class_desc, 
    null as  outlet_type,--not there
    'Unknown' as channel_code, 
    'Unknown' as channel_name, 
    null as business_channel,--not there
    null as loyalty_desc,--not there
    NULL as registration_date, 
    '1' as status_cd, 
    'Y' as status_desc, 
    'Unknown' as csrtrcode, 
    convert_timezone('UTC' , current_timestamp())::timestamp_ntz(9) as crt_dttm, 
    convert_timezone('UTC' , current_timestamp())::timestamp_ntz(9) as updt_dttm, 
    'Y' as actv_flg, 
    'Unknown' as retailer_Category_cd, 
    'Unknown' as Retailer_Category_Name, 
    'Unknown' as RtrLatitude, 
    'Unknown' as RtrLongitude, 
    'Unknown' as RtrUniquecode, 
    convert_timezone('UTC' , current_timestamp())::timestamp_ntz(9) as Createddate,
    null as  file_rec_dt, 
    'Unknown' as Type_Name,
    '-1' as town_code
    
  from source
),
final as 
(
    select * from temp_a 
    union all
    select * from temp_b
)
select * from final