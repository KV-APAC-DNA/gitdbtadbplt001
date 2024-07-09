with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_channel_mapping') }}
),
final as 
(
    select 
    code::number(18,0) as id,
	"channel name_code"::varchar(200) as channel_name,
	"retailer category name_code"::varchar(200) as retailer_category_name,
	"retailer class_code"::varchar(200) as retailer_class,
	"territory classification_code"::varchar(200) as territory_classification,
	"retailer - channel level 1_code"::varchar(200) as retailer_channel_level_1,
	"retailer - channel level 2_code"::varchar(200) as retailer_channel_level_2,
	"retailer - channel level 3_code"::varchar(200) as retailer_channel_level_3,
	report_channel_code::varchar(200) as report_channel,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
    lastchgdatetime::timestamp_ntz(9) as last_change_datetime,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final