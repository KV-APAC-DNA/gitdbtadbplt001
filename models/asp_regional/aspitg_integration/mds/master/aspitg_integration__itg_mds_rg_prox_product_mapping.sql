with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_prox_product_mapping') }}
),
final as
(
    SELECT 	code 
	,prox_product_code 
	,prox_product_name 
	,prox_brand_code 
	,prox_brand_name 
	,local_brand_code 
	,local_brand_name 
	,regional_brand_code 
	,regional_brand_name 
	,market
    ,category
    ,convert_timezone('UTC',current_timestamp()) as updt_dt
    FROM source
)
select code::varchar(500) as code,
    prox_product_code::varchar(200) as prox_product_code,
    prox_product_name::varchar(200) as prox_product_name,
    prox_brand_code::varchar(200) as prox_brand_code,
    prox_brand_name::varchar(200) as prox_brand_name,
    local_brand_code::varchar(200) as local_brand_code,
    local_brand_name::varchar(200) as local_brand_name,
    regional_brand_code::varchar(200) as regional_brand_code,
    regional_brand_name::varchar(200) as regional_brand_name,
    market::varchar(200) as market,
    category::varchar(200) as category,
    updt_dt::timestamp_ntz(9) as updt_dt
 from final