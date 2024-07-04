with source as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base') }}
),
final as 
(
    select 
    matl_num::varchar(18) as matl_num,
	chrt_acct::varchar(4) as chrt_acct,
	acct_num::varchar(10) as acct_num,
	dstr_chnl::varchar(2) as dstr_chnl,
	ctry_key::varchar(3) as ctry_key,
	caln_yr_mo::number(18,0) as caln_yr_mo,
	fisc_yr::number(18,0) as fisc_yr,
	fisc_yr_per::number(18,0) as fisc_yr_per,
	amt_obj_crncy::number(38,5) as amt_obj_crncy,
	qty::number(38,5) as qty,
	acct_hier_desc::varchar(100) as acct_hier_desc,
	acct_hier_shrt_desc::varchar(100) as acct_hier_shrt_desc,
	chnl_desc1::varchar(500) as chnl_desc1,
	chnl_desc2::varchar(500) as chnl_desc2,
	bw_gl::varchar(200) as bw_gl,
	nature::varchar(500) as nature,
	sap_gl::varchar(500) as sap_gl,
	descp::varchar(500) as descp,
	bravo_mapping::varchar(500) as bravo_mapping,
	sku_desc::varchar(500) as sku_desc,
	brand_combi::varchar(500) as brand_combi,
	franchise::varchar(500) as franchise,
	"group"::varchar(500) as "group",
	mrp::number(38,2) as mrp,
	cogs_per_unit::number(38,2) as cogs_per_unit,
	plan::varchar(10) as plan,
	brand_group_1::varchar(500) as brand_group_1,
	brand_group_2::varchar(500) as brand_group_2,
	co_cd::varchar(4) as co_cd,
	brand_combi_var::varchar(200) as brand_combi_var
    from source
)
select * from final