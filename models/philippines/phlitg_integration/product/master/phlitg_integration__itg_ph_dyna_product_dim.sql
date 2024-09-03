with source as(
	select * from {{ source('phlsdl_raw', 'sdl_ph_dyna_item_ref_dim') }}
),
final as(
	select 
		yearmo::varchar(20) as yearmonth,
		'DYNA'::varchar(20) as cust_cd,
		cust_item_cd::varchar(30) as item_cd,
		cust_item_desc::varchar(150) as item_nm,
		jj_item_cd::varchar(30) as sap_item_cd,
		cust_item_grp::varchar(150) as cust_sku_grp,
		conv_factor::varchar(20) as cust_conv_factor,
		null::number(10,2) as cust_item_prc,
		last_prd::varchar(10) as lst_period,
		early_bk_prd::varchar(10) as early_bk_period,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name
	from source
)
select * from final