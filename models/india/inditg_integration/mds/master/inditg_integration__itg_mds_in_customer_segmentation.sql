with source as
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_customer_segmentation') }}
),
final as
(
    select code as customer_code,
       name as customer_name,
	   customer_channel_code as customer_channel,
	   sap_channel_code as sap_channel,
	   region_name_name as region,
	   zone_name_name as zone,
	   territory_name_name as territory,
	   city_name as city,
	   type_name_code as customer_type,
	   customer_segmentation_level_1_code as customer_segmentation_level_1,
	   customer_segmentation_level_2_code as customer_segmentation_level_2,
	   convert_timezone('Asia/Singapore',current_timestamp())  as crtd_dttm
	   from source
)
select customer_code::varchar(20) as customer_code,
    customer_name::varchar(500) as customer_name,
    customer_channel::varchar(500) as customer_channel,
    sap_channel::varchar(500) as sap_channel,
    region::varchar(500) as region,
    zone::varchar(200) as zone,
    territory::varchar(200) as territory,
    city::varchar(200) as city,
    customer_type::varchar(200) as customer_type,
    customer_segmentation_level_1::varchar(200) as customer_segmentation_level_1,
    customer_segmentation_level_2::varchar(200) as customer_segmentation_level_2,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm
 from final