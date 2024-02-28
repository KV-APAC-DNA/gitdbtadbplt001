with source as (
    select * from {{ source('myssdl_raw','sdl_mds_my_gt_product_mapping') }}
),
transformed as (
	select
	  distributor_id as cust_id,
	  distributor_name as cust_nm,
	  sap_material_id as item_cd,
	  product_description as item_desc,
	  product_code as ext_item_cd,
	  null as cdl_dttm,
	  current_timestamp() as crtd_dttm,
	  null as updt_dttm
	from  source
	),
final as (
    select
    cust_id::varchar(50) as cust_id,
	cust_nm::varchar(100) as cust_nm,
	item_cd::varchar(50) as item_cd,
	item_desc::varchar(200) as item_desc,
	ext_item_cd::varchar(50) as ext_item_cd,
	cdl_dttm::varchar(50) as cdl_dttm,
	crtd_dttm::timestamp_ntz(9) as crtd_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
 from transformed
)
select * from final