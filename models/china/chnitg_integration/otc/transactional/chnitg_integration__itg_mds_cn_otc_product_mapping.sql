{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}};
        {% endif %}"
    )
}}
with sdl_mds_cn_otc_product_mapping as
(
    select * from {{source('chnsdl_raw', 'sdl_mds_cn_otc_product_mapping')}}
),
final as
(
    select
    name::varchar(500) as name,
	code::varchar(500) as code,
	xjp_code::varchar(200) as xjp_code,
	jntl_code::varchar(200) as jntl_code,
	brand_code::varchar(200) as brand_code,
	brand_cn::varchar(200) as brand_cn,
	brand_en::varchar(200) as brand_en,
	status::varchar(200) as status,
	product_name_cn::varchar(200) as product_name_cn,
	product_name_en::varchar(200) as product_name_en
    from sdl_mds_cn_otc_product_mapping
)
select * from final