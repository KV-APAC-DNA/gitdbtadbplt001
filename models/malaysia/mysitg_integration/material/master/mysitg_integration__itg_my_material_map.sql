with source as (
    select * from {{ source('myssdl_raw','sdl_mds_my_gt_product_mapping') }}
),

final as (
    select
    distributor_id::varchar(50) as cust_id,
	distributor_name::varchar(100) as cust_nm,
	sap_material_id::varchar(50) as item_cd,
	product_description::varchar(200) as item_desc,
	product_code::varchar(50) as ext_item_cd,
	null::varchar(50) as cdl_dttm,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	null::timestamp_ntz(9) as updt_dttm
 from source
)

select * from final