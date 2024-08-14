with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_prox_channel_mapping') }}
),
final as
(
    SELECT 	code
	,prox_channel_code
	,prox_channel_name
	,regional_channel_code
	,regional_channel_name
	,local_channel_code
	,local_channel_name
	,market
    ,convert_timezone('UTC',current_timestamp()) as updt_dt 
    FROM source
)
select code::varchar(500) as code,
    prox_channel_code::varchar(200) as prox_channel_code,
    prox_channel_name::varchar(200) as prox_channel_name,
    regional_channel_code::varchar(200) as regional_channel_code,
    regional_channel_name::varchar(200) as regional_channel_name,
    local_channel_code::varchar(200) as local_channel_code,
    local_channel_name::varchar(200) as local_channel_name,
    market::varchar(200) as market,
    updt_dt::timestamp_ntz(9) as updt_dt
 from final