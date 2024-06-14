with source as 
(
    select * from {{ ref('idnitg_integration__itg_product_dim') }}
),
final as
(
    select
    jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
	jj_sap_prod_desc::varchar(100) as jj_sap_prod_desc,
	franchise::varchar(50) as franchise,
	brand::varchar(50) as brand,
	variant1::varchar(50) as variant1,
	variant2::varchar(50) as variant2,
	variant3::varchar(50) as variant3,
	status::varchar(50) as status,
	put_up::number(18,0) as put_up,
	uom::number(18,0) as uom,
	jj_sap_upgrd_prod_id::varchar(50) as jj_sap_upgrd_prod_id,
	jj_sap_upgrd_prod_desc::varchar(100) as jj_sap_upgrd_prod_desc,
	price::number(18,0) as price,
	prod_class::number(18,0) as prod_class,
	jj_sap_cd_mp_prod_id::varchar(50) as jj_sap_cd_mp_prod_id,
	jj_sap_cd_mp_prod_desc::varchar(100) as jj_sap_cd_mp_prod_desc,
	price_vmr::number(18,0) as price_vmr,
	pft_sm::varchar(60) as pft_sm,
	pft_mm::varchar(60) as pft_mm,
	pft_ws::varchar(60) as pft_ws,
	pft_prov::varchar(60) as pft_prov,
	pft_ds::varchar(60) as pft_ds,
	pft_mws::varchar(60) as pft_mws,
	pft_apt::varchar(60) as pft_apt,
	pft_bs::varchar(60) as pft_bs,
	pft_cs::varchar(60) as pft_cs,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as uptd_dttm,
	effective_from::varchar(10) as effective_from,
	effective_to::varchar(10) as effective_to
    from source
)
select * from final