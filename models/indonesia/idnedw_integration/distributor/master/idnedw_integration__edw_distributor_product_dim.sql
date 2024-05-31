with source as 
(
    select * from {{ ref('idnitg_integration__itg_distributor_product_dim') }}
),
final as 
(
    select
    dstrbtr_grp_cd::varchar(15) as dstrbtr_grp_cd,
	dstrbtr_id::varchar(50) as dstrbtr_id,
	dstrbtr_prod_id::varchar(50) as dstrbtr_prod_id,
	jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
	jj_sap_prod_desc::varchar(255) as jj_sap_prod_desc,
	franchise::varchar(50) as franchise,
	brand::varchar(25) as brand,
	cse::number(18,0) as cse,
	prod_key::varchar(100) as prod_key,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
	denominator::number(20,0) as denominator,
	effective_from::varchar(10) as effective_from,
	effective_to::varchar(10) as effective_to
    from source
)
select * from final