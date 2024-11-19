with edw_rpt_copa_customergp_agg as
(
    select * from {{ ref('aspedw_integration__edw_rpt_copa_customergp_agg') }}
),
itg_custpl_excp_ctrl as
(
    select * from {{ source('aspitg_integration','itg_custpl_excp_ctrl_temp') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
vw_itg_custgp_prod_hierarchy as
(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_prod_hierarchy') }}
),
vw_itg_custgp_customer_hierarchy as
(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_customer_hierarchy') }}
),
itg_mds_custgp_portfolio_mapping as
(
    select * from {{ ref('aspitg_integration__itg_mds_custgp_portfolio_mapping') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
wks_rpt_copa_customergp_agg2 as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_agg2') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
itg_custgp_cogs_fg_control as
(
    select * from {{ source('aspitg_integration', 'itg_custgp_cogs_fg_control') }}
),
edw_custgp_cogs_automation as
(
    select * from {{ ref('aspedw_integration__edw_custgp_cogs_automation') }}
),
itg_mds_pre_apsc_master as
(
    select * from {{ ref('aspitg_integration__itg_mds_pre_apsc_master') }}
),

final as
(
    select distinct 'incomplete regional customer hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_centr,		--//        custgp.prft_ctr as profit_centr,
       exc.error_desc||' for customer number '||custgp.cust_num as error_desc,		--//        exc.error_desc||' for customer number '||custgp.cust_num as error_desc,
       exc.corrective_action||' for customer number '||custgp.cust_num as corrective_action		--//        exc.corrective_action||' for customer number '||custgp.cust_num as corrective_action
from edw_rpt_copa_customergp_agg custgp, itg_custpl_excp_ctrl exc		--// from rg_edw.edw_rpt_copa_customergp_agg custgp, rg_itg.itg_custpl_excp_ctrl exc
where
((case
when (coalesce(nullif(custgp.retail_env,''),'na') = 'na' or custgp.retail_env is null or custgp.retail_env = 'not available')		--// when (coalesce(nullif(custgp.retail_env,''),'na') = 'na' or custgp.retail_env is null or custgp.retail_env = 'not available')
then 'regchnl1'
when (coalesce(nullif(custgp."sub channel",''),'na') = 'na' or custgp."sub channel" is null or custgp."sub channel" = 'not available')		--// when (coalesce(nullif(custgp."sub channel",''),'na') = 'na' or custgp."sub channel" is null or custgp."sub channel" = 'not available')
then 'regchnl2'
when (coalesce(nullif(custgp."go to model",''),'na') = 'na' or custgp."go to model" is null or custgp."go to model" = 'not available')		--// when (coalesce(nullif(custgp."go to model",''),'na') = 'na' or custgp."go to model" is null or custgp."go to model" = 'not available')
then 'regchnl3'
when (coalesce(nullif(custgp."parent customer",''),'na') = 'na' or custgp."parent customer" is null or custgp."parent customer" = 'not available')		--// when (coalesce(nullif(custgp."parent customer",''),'na') = 'na' or custgp."parent customer" is null or custgp."parent customer" = 'not available')
then 'regcust1'
when (coalesce(nullif(custgp.banner,''),'na') = 'na' or custgp.banner is null or custgp.banner = 'not available')		--// when (coalesce(nullif(custgp.banner,''),'na') = 'na' or custgp.banner is null or custgp.banner = 'not available')
then 'regcust2'
when (coalesce(nullif(custgp."banner format",''),'na') = 'na' or custgp."banner format" is null or custgp."banner format" = 'not available')		--// when (coalesce(nullif(custgp."banner format",''),'na') = 'na' or custgp."banner format" is null or custgp."banner format" = 'not available')
then 'regcust3'
end = exc.prodkey) and custgp.ctry_nm = exc.ctry_nm)		--// end = exc.prodkey) and custgp.ctry_nm = exc.ctry_nm)
and (
(coalesce(nullif(custgp.retail_env,''),'na') = 'na' or custgp.retail_env is null or custgp.retail_env = 'not available') or		--// (coalesce(nullif(custgp.retail_env,''),'na') = 'na' or custgp.retail_env is null or custgp.retail_env = 'not available') or
(coalesce(nullif(custgp."sub channel",''),'na') = 'na' or custgp."sub channel" is null or custgp."sub channel" = 'not available') or		--// (coalesce(nullif(custgp."sub channel",''),'na') = 'na' or custgp."sub channel" is null or custgp."sub channel" = 'not available') or
(coalesce(nullif(custgp."go to model",''),'na') = 'na' or custgp."go to model" is null or custgp."go to model" = 'not available') or		--// (coalesce(nullif(custgp."go to model",''),'na') = 'na' or custgp."go to model" is null or custgp."go to model" = 'not available') or
(coalesce(nullif(custgp."parent customer",''),'na') = 'na' or custgp."parent customer" is null  or custgp."parent customer" = 'not available') or		--// (coalesce(nullif(custgp."parent customer",''),'na') = 'na' or custgp."parent customer" is null  or custgp."parent customer" = 'not available') or
(coalesce(nullif(custgp.banner,''),'na') = 'na' or custgp.banner is null or custgp.banner = 'not available') or		--// (coalesce(nullif(custgp.banner,''),'na') = 'na' or custgp.banner is null or custgp.banner = 'not available') or
(coalesce(nullif(custgp."banner format",''),'na') = 'na' or custgp."banner format" is null or custgp."banner format" = 'not available'))		--// (coalesce(nullif(custgp."banner format",''),'na') = 'na' or custgp."banner format" is null or custgp."banner format" = 'not available'))
and (custgp.cust_num != '' and custgp.cust_num != 'not available' and custgp.cust_num is not null)		--// and (custgp.cust_num != '' and custgp.cust_num != 'not available' and custgp.cust_num is not null)
union all
select distinct 'missing sold-to code in regional customer hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       'sold-to code '||custgp.cust_num||' missing from sap customer sales master' as error_desc,		--//        'sold-to code '||custgp.cust_num||' missing from sap customer sales master' as error_desc,
       'please add an entry for sold-to code '||custgp.cust_num||' in sap customer sales master for sls_org ' || custgp.sls_org as corrective_action		--//        'please add an entry for sold-to code '||custgp.cust_num||' in sap customer sales master for sls_org ' || custgp.sls_org as corrective_action
from wks_rpt_copa_customergp_agg2 custgp		--// from rg_wks.wks_rpt_copa_customergp_agg2 custgp
left join
(select sls_org,cust_num,
							 "parent customer",
							banner,
							"banner format",
							channel,
							"go to model",
							"sub channel",
							retail_env

	from
	(select sls_org,ltrim(cust_num,'0') as cust_num,
							 "parent customer",
							banner,
							"banner format",
							channel,
							"go to model",
							"sub channel",
							retail_env,
							row_number() over( partition by sls_org,cust_num
				                               order by prnt_cust_key desc) rn
	from v_edw_customer_sales_dim )where rn = 1) cust		--// 	from rg_edw.v_edw_customer_sales_dim )where rn = 1) cust
		on custgp.sls_org::text = cust.sls_org::text		--// 		on custgp.sls_org::text = cust.sls_org::text
        and custgp.cust_num::text = cust.cust_num		--//         and custgp.cust_num::text = cust.cust_num
where (cust.cust_num = '' or cust.cust_num is null) and (custgp.cust_num != '' or custgp.cust_num != 'not available' or custgp.cust_num is not null)		--// where (cust.cust_num = '' or cust.cust_num is null) and (custgp.cust_num != '' or custgp.cust_num != 'not available' or custgp.cust_num is not null)
union all
select distinct 'incomplete regional product hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for material '||custgp.matl_num as error_desc,		--//        exc.error_desc||' for material '||custgp.matl_num as error_desc,
       exc.corrective_action||' for material '||custgp.matl_num as corrective_action		--//        exc.corrective_action||' for material '||custgp.matl_num as corrective_action
from edw_rpt_copa_customergp_agg custgp, itg_custpl_excp_ctrl exc		--// from rg_edw.edw_rpt_copa_customergp_agg custgp, rg_itg.itg_custpl_excp_ctrl exc
where (custgp.matl_num != 'rebate' and custgp.matl_num != '' and custgp.matl_num is not null and custgp.matl_num != 'not available' )		--// where (custgp.matl_num != 'rebate' and custgp.matl_num != '' and custgp.matl_num is not null and custgp.matl_num != 'not available' )
and
((case
when (coalesce(nullif(custgp.gcph_franchise,''),'na') = 'na' or custgp.gcph_franchise is null or custgp.gcph_franchise = 'not available')		--// when (coalesce(nullif(custgp.gcph_franchise,''),'na') = 'na' or custgp.gcph_franchise is null or custgp.gcph_franchise = 'not available')
then 'regprod1'
when (coalesce(nullif(custgp.sap_frnchse_desc,''),'na') = 'na' or custgp.sap_frnchse_desc is null or custgp.sap_frnchse_desc = 'not available')		--// when (coalesce(nullif(custgp.sap_frnchse_desc,''),'na') = 'na' or custgp.sap_frnchse_desc is null or custgp.sap_frnchse_desc = 'not available')
then 'regprod2'
when (coalesce(nullif(custgp.mega_brnd_desc,''),'na') = 'na' or custgp.mega_brnd_desc is null or custgp.mega_brnd_desc = 'not available')		--// when (coalesce(nullif(custgp.mega_brnd_desc,''),'na') = 'na' or custgp.mega_brnd_desc is null or custgp.mega_brnd_desc = 'not available')
then 'regprod3'
when (coalesce(nullif(custgp.brnd_desc,''),'na') = 'na' or custgp.brnd_desc is null or custgp.brnd_desc = 'not available')		--// when (coalesce(nullif(custgp.brnd_desc,''),'na') = 'na' or custgp.brnd_desc is null or custgp.brnd_desc = 'not available')
then 'regprod4'
when (coalesce(nullif(custgp.pka_sub_brand_desc,''),'na') = 'na' or custgp.pka_sub_brand_desc is null or custgp.pka_sub_brand_desc = 'not available')		--// when (coalesce(nullif(custgp.pka_sub_brand_desc,''),'na') = 'na' or custgp.pka_sub_brand_desc is null or custgp.pka_sub_brand_desc = 'not available')
then 'regprod5'
when (coalesce(nullif(custgp.sap_prod_mnr_desc,''),'na') = 'na' or custgp.sap_prod_mnr_desc is null or custgp.sap_prod_mnr_desc = 'not available')		--// when (coalesce(nullif(custgp.sap_prod_mnr_desc,''),'na') = 'na' or custgp.sap_prod_mnr_desc is null or custgp.sap_prod_mnr_desc = 'not available')
then 'regprod6'
when (coalesce(nullif(custgp.varnt_desc,''),'na') = 'na' or custgp.varnt_desc is null or custgp.varnt_desc = 'not available')		--// when (coalesce(nullif(custgp.varnt_desc,''),'na') = 'na' or custgp.varnt_desc is null or custgp.varnt_desc = 'not available')
then 'regprod7'
when (coalesce(nullif(custgp.pka_size_desc,''),'na') = 'na' or custgp.pka_size_desc is null or custgp.pka_size_desc = 'not available')		--// when (coalesce(nullif(custgp.pka_size_desc,''),'na') = 'na' or custgp.pka_size_desc is null or custgp.pka_size_desc = 'not available')
then 'regprod8'
when (coalesce(nullif(custgp.pka_package_desc,''),'na') = 'na' or custgp.pka_package_desc is null or custgp.pka_package_desc = 'not available')		--// when (coalesce(nullif(custgp.pka_package_desc,''),'na') = 'na' or custgp.pka_package_desc is null or custgp.pka_package_desc = 'not available')
then 'regprod9'
when (coalesce(nullif(custgp.pka_product_key_description,''),'na') = 'na' or custgp.pka_product_key_description is null or custgp.pka_product_key_description = 'not available')		--// when (coalesce(nullif(custgp.pka_product_key_description,''),'na') = 'na' or custgp.pka_product_key_description is null or custgp.pka_product_key_description = 'not available')
then 'regprod10'
end = exc.prodkey) and custgp.ctry_nm = exc.ctry_nm) and		--// end = exc.prodkey) and custgp.ctry_nm = exc.ctry_nm) and
(
(coalesce(nullif(custgp.gcph_franchise,''),'na') = 'na' or custgp.gcph_franchise is null or custgp.gcph_franchise = 'not available') or		--// (coalesce(nullif(custgp.gcph_franchise,''),'na') = 'na' or custgp.gcph_franchise is null or custgp.gcph_franchise = 'not available') or
(coalesce(nullif(custgp.sap_frnchse_desc,''),'na') = 'na' or custgp.sap_frnchse_desc is null or custgp.sap_frnchse_desc = 'not available') or		--// (coalesce(nullif(custgp.sap_frnchse_desc,''),'na') = 'na' or custgp.sap_frnchse_desc is null or custgp.sap_frnchse_desc = 'not available') or
(coalesce(nullif(custgp.mega_brnd_desc,''),'na') = 'na' or custgp.mega_brnd_desc is null or custgp.mega_brnd_desc = 'not available') or		--// (coalesce(nullif(custgp.mega_brnd_desc,''),'na') = 'na' or custgp.mega_brnd_desc is null or custgp.mega_brnd_desc = 'not available') or
(coalesce(nullif(custgp.brnd_desc,''),'na') = 'na' or custgp.brnd_desc is null  or custgp.brnd_desc = 'not available') or		--// (coalesce(nullif(custgp.brnd_desc,''),'na') = 'na' or custgp.brnd_desc is null  or custgp.brnd_desc = 'not available') or
(coalesce(nullif(custgp.pka_sub_brand_desc,''),'na') = 'na' or custgp.pka_sub_brand_desc is null or custgp.pka_sub_brand_desc = 'not available') or		--// (coalesce(nullif(custgp.pka_sub_brand_desc,''),'na') = 'na' or custgp.pka_sub_brand_desc is null or custgp.pka_sub_brand_desc = 'not available') or
(coalesce(nullif(custgp.sap_prod_mnr_desc,''),'na') = 'na' or custgp.sap_prod_mnr_desc is null or custgp.sap_prod_mnr_desc = 'not available') or		--// (coalesce(nullif(custgp.sap_prod_mnr_desc,''),'na') = 'na' or custgp.sap_prod_mnr_desc is null or custgp.sap_prod_mnr_desc = 'not available') or
(coalesce(nullif(custgp.varnt_desc,''),'na') = 'na' or custgp.varnt_desc is null or custgp.varnt_desc = 'not available') or		--// (coalesce(nullif(custgp.varnt_desc,''),'na') = 'na' or custgp.varnt_desc is null or custgp.varnt_desc = 'not available') or
(coalesce(nullif(custgp.pka_size_desc,''),'na') = 'na' or custgp.pka_size_desc is null or custgp.pka_size_desc = 'not available') or		--// (coalesce(nullif(custgp.pka_size_desc,''),'na') = 'na' or custgp.pka_size_desc is null or custgp.pka_size_desc = 'not available') or
(coalesce(nullif(custgp.pka_package_desc,''),'na') = 'na' or custgp.pka_package_desc is null or custgp.pka_package_desc = 'not available') or		--// (coalesce(nullif(custgp.pka_package_desc,''),'na') = 'na' or custgp.pka_package_desc is null or custgp.pka_package_desc = 'not available') or
(coalesce(nullif(custgp.pka_product_key_description,''),'na') = 'na' or custgp.pka_product_key_description is null or custgp.pka_product_key_description = 'not available')		--// (coalesce(nullif(custgp.pka_product_key_description,''),'na') = 'na' or custgp.pka_product_key_description is null or custgp.pka_product_key_description = 'not available')
)
union all
select distinct 'missing regional product hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       'missing material '||custgp.matl_num as error_desc,		--//        'missing material '||custgp.matl_num as error_desc,
       'material ' ||custgp.matl_num|| ' needs to be added in sap material master' as corrective_action		--//        'material ' ||custgp.matl_num|| ' needs to be added in sap material master' as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
left join edw_material_dim mat on custgp.matl_num = ltrim(mat.matl_num,'0')		--// left join rg_edw.edw_material_dim mat on custgp.matl_num = ltrim(mat.matl_num,'0')
where (custgp.matl_num is not null and custgp.matl_num != '' and custgp.matl_num != 'not available')		--// where (custgp.matl_num is not null and custgp.matl_num != '' and custgp.matl_num != 'not available')
and (mat.matl_num is null or mat.matl_num = '')		--// and (mat.matl_num is null or mat.matl_num = '')
union all
select distinct 'missing regional product hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       'missing material '||custgp.matl_num as error_desc,		--//        'missing material '||custgp.matl_num as error_desc,
       'material ' ||custgp.matl_num|| ' needs to be added in gcph product hierarchy' as corrective_action		--//        'material ' ||custgp.matl_num|| ' needs to be added in gcph product hierarchy' as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
left join edw_gch_producthierarchy gch on custgp.matl_num = ltrim(gch.materialnumber,'0')		--// left join rg_edw.edw_gch_producthierarchy gch on custgp.matl_num = ltrim(gch.materialnumber,'0')
where (custgp.matl_num is not null and custgp.matl_num != '' and custgp.matl_num != 'not available')		--// where (custgp.matl_num is not null and custgp.matl_num != '' and custgp.matl_num != 'not available')
and (gch.materialnumber is null or gch.materialnumber = '')		--// and (gch.materialnumber is null or gch.materialnumber = '')
union all
select distinct 'incomplete local product hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for material '||custgp.matl_num as error_desc,		--//        exc.error_desc||' for material '||custgp.matl_num as error_desc,
       exc.corrective_action||' for material '||custgp.matl_num as corrective_action		--//        exc.corrective_action||' for material '||custgp.matl_num as corrective_action
from edw_rpt_copa_customergp_agg custgp,		--// from rg_edw.edw_rpt_copa_customergp_agg custgp,
vw_itg_custgp_prod_hierarchy locprod,
itg_custpl_excp_ctrl exc
where (custgp.ctry_nm = locprod.ctry_nm and custgp.matl_num = locprod.matl_num) and		--// where (custgp.ctry_nm = locprod.ctry_nm and custgp.matl_num = locprod.matl_num) and
(custgp.matl_num != 'rebate' and custgp.matl_num != '' and custgp.matl_num != 'not available') and		--// (custgp.matl_num != 'rebate' and custgp.matl_num != '' and custgp.matl_num != 'not available') and
((case
when (coalesce(nullif(custgp.loc_prod1,''),'na') = 'na' or custgp.loc_prod1 is null or custgp.loc_prod1 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod1,''),'na') = 'na' or custgp.loc_prod1 is null or custgp.loc_prod1 = 'not available')
then 'locprod1'
when (coalesce(nullif(custgp.loc_prod2,''),'na') = 'na' or custgp.loc_prod2 is null or custgp.loc_prod2 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod2,''),'na') = 'na' or custgp.loc_prod2 is null or custgp.loc_prod2 = 'not available')
then 'locprod2'
when (coalesce(nullif(custgp.loc_prod3,''),'na') = 'na' or custgp.loc_prod3 is null or custgp.loc_prod3 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod3,''),'na') = 'na' or custgp.loc_prod3 is null or custgp.loc_prod3 = 'not available')
then 'locprod3'
when (coalesce(nullif(custgp.loc_prod4,''),'na') = 'na' or custgp.loc_prod4 is null or custgp.loc_prod4 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod4,''),'na') = 'na' or custgp.loc_prod4 is null or custgp.loc_prod4 = 'not available')
then 'locprod4'
when (coalesce(nullif(custgp.loc_prod5,''),'na') = 'na' or custgp.loc_prod5 is null or custgp.loc_prod5 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod5,''),'na') = 'na' or custgp.loc_prod5 is null or custgp.loc_prod5 = 'not available')
then 'locprod5'
when (coalesce(nullif(custgp.loc_prod6,''),'na') = 'na' or custgp.loc_prod6 is null or custgp.loc_prod6 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod6,''),'na') = 'na' or custgp.loc_prod6 is null or custgp.loc_prod6 = 'not available')
then 'locprod6'
when (coalesce(nullif(custgp.loc_prod7,''),'na') = 'na' or custgp.loc_prod7 is null or custgp.loc_prod7 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod7,''),'na') = 'na' or custgp.loc_prod7 is null or custgp.loc_prod7 = 'not available')
then 'locprod7'
when (coalesce(nullif(custgp.loc_prod8,''),'na') = 'na' or custgp.loc_prod8 is null or custgp.loc_prod8 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod8,''),'na') = 'na' or custgp.loc_prod8 is null or custgp.loc_prod8 = 'not available')
then 'locprod8'
when (coalesce(nullif(custgp.loc_prod9,''),'na') = 'na' or custgp.loc_prod9 is null or custgp.loc_prod9 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod9,''),'na') = 'na' or custgp.loc_prod9 is null or custgp.loc_prod9 = 'not available')
then 'locprod9'
when (coalesce(nullif(custgp.loc_prod10,''),'na') = 'na' or custgp.loc_prod10 is null or custgp.loc_prod10 = 'not available')		--// when (coalesce(nullif(custgp.loc_prod10,''),'na') = 'na' or custgp.loc_prod10 is null or custgp.loc_prod10 = 'not available')
then 'locprod10'
end = exc.prodkey) and custgp.ctry_nm = exc.ctry_nm) and		--// end = exc.prodkey) and custgp.ctry_nm = exc.ctry_nm) and
(
(coalesce(nullif(custgp.loc_prod1,''),'na') = 'na' or custgp.loc_prod1 is null or custgp.loc_prod1 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod1,''),'na') = 'na' or custgp.loc_prod1 is null or custgp.loc_prod1 = 'not available') or
(coalesce(nullif(custgp.loc_prod2,''),'na') = 'na' or custgp.loc_prod2 is null or custgp.loc_prod2 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod2,''),'na') = 'na' or custgp.loc_prod2 is null or custgp.loc_prod2 = 'not available') or
(coalesce(nullif(custgp.loc_prod3,''),'na') = 'na' or custgp.loc_prod3 is null or custgp.loc_prod3 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod3,''),'na') = 'na' or custgp.loc_prod3 is null or custgp.loc_prod3 = 'not available') or
(coalesce(nullif(custgp.loc_prod4,''),'na') = 'na' or custgp.loc_prod4 is null  or custgp.loc_prod4 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod4,''),'na') = 'na' or custgp.loc_prod4 is null  or custgp.loc_prod4 = 'not available') or
(coalesce(nullif(custgp.loc_prod5,''),'na') = 'na' or custgp.loc_prod5 is null or custgp.loc_prod5 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod5,''),'na') = 'na' or custgp.loc_prod5 is null or custgp.loc_prod5 = 'not available') or
(coalesce(nullif(custgp.loc_prod6,''),'na') = 'na' or custgp.loc_prod6 is null or custgp.loc_prod6 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod6,''),'na') = 'na' or custgp.loc_prod6 is null or custgp.loc_prod6 = 'not available') or
(coalesce(nullif(custgp.loc_prod7,''),'na') = 'na' or custgp.loc_prod7 is null or custgp.loc_prod7 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod7,''),'na') = 'na' or custgp.loc_prod7 is null or custgp.loc_prod7 = 'not available') or
(coalesce(nullif(custgp.loc_prod8,''),'na') = 'na' or custgp.loc_prod8 is null or custgp.loc_prod8 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod8,''),'na') = 'na' or custgp.loc_prod8 is null or custgp.loc_prod8 = 'not available') or
(coalesce(nullif(custgp.loc_prod9,''),'na') = 'na' or custgp.loc_prod9 is null or custgp.loc_prod9 = 'not available') or		--// (coalesce(nullif(custgp.loc_prod9,''),'na') = 'na' or custgp.loc_prod9 is null or custgp.loc_prod9 = 'not available') or
(coalesce(nullif(custgp.loc_prod10,''),'na') = 'na' or custgp.loc_prod10 is null or custgp.loc_prod10 = 'not available')		--// (coalesce(nullif(custgp.loc_prod10,''),'na') = 'na' or custgp.loc_prod10 is null or custgp.loc_prod10 = 'not available')
)
union all
select distinct 'missing local product hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for '||custgp.matl_num as error_desc,		--//        exc.error_desc||' for '||custgp.matl_num as error_desc,
       exc.corrective_action ||' for '||custgp.matl_num as corrective_action		--//        exc.corrective_action ||' for '||custgp.matl_num as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
join itg_custpl_excp_ctrl exc on custgp.ctry_nm = exc.ctry_nm and 'mismatl' = exc.prodkey		--// join rg_itg.itg_custpl_excp_ctrl exc on custgp.ctry_nm = exc.ctry_nm and 'mismatl' = exc.prodkey
left join vw_itg_custgp_prod_hierarchy locprod on		--// left join rg_itg.vw_itg_custgp_prod_hierarchy locprod on
(custgp.ctry_nm = locprod.ctry_nm and custgp.cust_num = locprod.matl_num)		--// (custgp.ctry_nm = locprod.ctry_nm and custgp.cust_num = locprod.matl_num)
where (custgp.matl_num != 'not available' and custgp.matl_num != '' and custgp.matl_num is not null) and (locprod.matl_num = '' or locprod.matl_num is null or locprod.matl_num = 'not available')		--// where (custgp.matl_num != 'not available' and custgp.matl_num != '' and custgp.matl_num is not null) and (locprod.matl_num = '' or locprod.matl_num is null or locprod.matl_num = 'not available')
union all
select distinct 'incomplete local customer hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for customer number '||custgp.cust_num as error_desc,		--//        exc.error_desc||' for customer number '||custgp.cust_num as error_desc,
       exc.corrective_action||' for customer number '||custgp.cust_num as corrective_action		--//        exc.corrective_action||' for customer number '||custgp.cust_num as corrective_action
from edw_rpt_copa_customergp_agg custgp, itg_custpl_excp_ctrl exc,vw_itg_custgp_customer_hierarchy loccust		--// from rg_edw.edw_rpt_copa_customergp_agg custgp, rg_itg.itg_custpl_excp_ctrl exc,rg_itg.vw_itg_custgp_customer_hierarchy loccust
where (custgp.cust_num != 'not available' and custgp.cust_num != '' and custgp.cust_num is not null) and		--// where (custgp.cust_num != 'not available' and custgp.cust_num != '' and custgp.cust_num is not null) and
(custgp.ctry_nm = loccust.ctry_nm and custgp.cust_num = loccust.cust_num) and		--// (custgp.ctry_nm = loccust.ctry_nm and custgp.cust_num = loccust.cust_num) and
((case
when (coalesce(nullif(custgp.loc_channel1,''),'na') = 'na' or custgp.loc_channel1 is null or custgp.loc_channel1 = 'not available')		--// when (coalesce(nullif(custgp.loc_channel1,''),'na') = 'na' or custgp.loc_channel1 is null or custgp.loc_channel1 = 'not available')
then 'locchnl1'
when (coalesce(nullif(custgp.loc_channel2,''),'na') = 'na' or custgp.loc_channel2 is null or custgp.loc_channel2 = 'not available')		--// when (coalesce(nullif(custgp.loc_channel2,''),'na') = 'na' or custgp.loc_channel2 is null or custgp.loc_channel2 = 'not available')
then 'locchnl2'
when (coalesce(nullif(custgp.loc_channel3,''),'na') = 'na' or custgp.loc_channel3 is null or custgp.loc_channel3 = 'not available')		--// when (coalesce(nullif(custgp.loc_channel3,''),'na') = 'na' or custgp.loc_channel3 is null or custgp.loc_channel3 = 'not available')
then 'locchnl3'
when (coalesce(nullif(custgp.loc_cust1,''),'na') = 'na' or custgp.loc_cust1 is null or custgp.loc_cust1 = 'not available')		--// when (coalesce(nullif(custgp.loc_cust1,''),'na') = 'na' or custgp.loc_cust1 is null or custgp.loc_cust1 = 'not available')
then 'loccust1'
when (coalesce(nullif(custgp.loc_cust2,''),'na') = 'na' or custgp.loc_cust2 is null or custgp.loc_cust2 = 'not available')		--// when (coalesce(nullif(custgp.loc_cust2,''),'na') = 'na' or custgp.loc_cust2 is null or custgp.loc_cust2 = 'not available')
then 'loccust2'
when (coalesce(nullif(custgp.loc_cust3,''),'na') = 'na' or custgp.loc_cust3 is null or custgp.loc_cust3 = 'not available')		--// when (coalesce(nullif(custgp.loc_cust3,''),'na') = 'na' or custgp.loc_cust3 is null or custgp.loc_cust3 = 'not available')
then 'loccust3'
when (coalesce(nullif(custgp.local_cust_segmentation,''),'na') = 'na' or custgp.local_cust_segmentation is null or custgp.local_cust_segmentation = 'not available')		--// when (coalesce(nullif(custgp.local_cust_segmentation,''),'na') = 'na' or custgp.local_cust_segmentation is null or custgp.local_cust_segmentation = 'not available')
then 'locseg1'
when (coalesce(nullif(custgp.local_cust_segmentation_2,''),'na') = 'na' or custgp.local_cust_segmentation_2 is null or custgp.local_cust_segmentation_2 = 'not available')		--// when (coalesce(nullif(custgp.local_cust_segmentation_2,''),'na') = 'na' or custgp.local_cust_segmentation_2 is null or custgp.local_cust_segmentation_2 = 'not available')
then 'locseg2'
end = exc.prodkey) and  custgp.ctry_nm = exc.ctry_nm) and		--// end = exc.prodkey) and  custgp.ctry_nm = exc.ctry_nm) and
(
(coalesce(nullif(custgp.loc_channel1,''),'na') = 'na' or custgp.loc_channel1 is null or custgp.loc_channel1 = 'not available') or		--// (coalesce(nullif(custgp.loc_channel1,''),'na') = 'na' or custgp.loc_channel1 is null or custgp.loc_channel1 = 'not available') or
(coalesce(nullif(custgp.loc_channel2,''),'na') = 'na' or custgp.loc_channel2 is null or custgp.loc_channel2 = 'not available') or		--// (coalesce(nullif(custgp.loc_channel2,''),'na') = 'na' or custgp.loc_channel2 is null or custgp.loc_channel2 = 'not available') or
(coalesce(nullif(custgp.loc_channel3,''),'na') = 'na' or custgp.loc_channel3 is null or custgp.loc_channel3 = 'not available') or		--// (coalesce(nullif(custgp.loc_channel3,''),'na') = 'na' or custgp.loc_channel3 is null or custgp.loc_channel3 = 'not available') or
(coalesce(nullif(custgp.loc_cust1,''),'na') = 'na' or custgp.loc_cust1 is null  or custgp.loc_cust1 = 'not available') or		--// (coalesce(nullif(custgp.loc_cust1,''),'na') = 'na' or custgp.loc_cust1 is null  or custgp.loc_cust1 = 'not available') or
(coalesce(nullif(custgp.loc_cust2,''),'na') = 'na' or custgp.loc_cust2 is null or custgp.loc_cust2 = 'not available') or		--// (coalesce(nullif(custgp.loc_cust2,''),'na') = 'na' or custgp.loc_cust2 is null or custgp.loc_cust2 = 'not available') or
(coalesce(nullif(custgp.loc_cust3,''),'na') = 'na' or custgp.loc_cust3 is null or custgp.loc_cust3 = 'not available') or		--// (coalesce(nullif(custgp.loc_cust3,''),'na') = 'na' or custgp.loc_cust3 is null or custgp.loc_cust3 = 'not available') or
(coalesce(nullif(custgp.local_cust_segmentation,''),'na') = 'na' or custgp.local_cust_segmentation is null or custgp.local_cust_segmentation = 'not available') or		--// (coalesce(nullif(custgp.local_cust_segmentation,''),'na') = 'na' or custgp.local_cust_segmentation is null or custgp.local_cust_segmentation = 'not available') or
(coalesce(nullif(custgp.local_cust_segmentation_2,''),'na') = 'na' or custgp.local_cust_segmentation_2 is null or custgp.local_cust_segmentation_2 = 'not available')		--// (coalesce(nullif(custgp.local_cust_segmentation_2,''),'na') = 'na' or custgp.local_cust_segmentation_2 is null or custgp.local_cust_segmentation_2 = 'not available')
)
union all
select distinct 'missing sold-to code in local customer hierarchy' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' '||custgp.cust_num as error_desc,		--//        exc.error_desc||' '||custgp.cust_num as error_desc,
       exc.corrective_action ||' for '||custgp.cust_num as corrective_action		--//        exc.corrective_action ||' for '||custgp.cust_num as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
join itg_custpl_excp_ctrl exc on custgp.ctry_nm = exc.ctry_nm and 'missoldto' = exc.prodkey		--// join rg_itg.itg_custpl_excp_ctrl exc on custgp.ctry_nm = exc.ctry_nm and 'missoldto' = exc.prodkey
left join vw_itg_custgp_customer_hierarchy loccust on		--// left join rg_itg.vw_itg_custgp_customer_hierarchy loccust on
(custgp.ctry_nm = loccust.ctry_nm and custgp.cust_num = loccust.cust_num)		--// (custgp.ctry_nm = loccust.ctry_nm and custgp.cust_num = loccust.cust_num)
where (custgp.cust_num != 'not available' and custgp.cust_num != '' and custgp.cust_num is not null) and		--// where (custgp.cust_num != 'not available' and custgp.cust_num != '' and custgp.cust_num is not null) and
 (loccust.cust_num = '' or loccust.cust_num is null or loccust.cust_num = 'not available')		--//  (loccust.cust_num = '' or loccust.cust_num is null or loccust.cust_num = 'not available')
union all
select distinct 'incomplete regional product portfolio' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for '||custgp.prft_ctr as error_desc,		--//        exc.error_desc||' for '||custgp.prft_ctr as error_desc,
       exc.corrective_action ||' for '||custgp.prft_ctr as corrective_action		--//        exc.corrective_action ||' for '||custgp.prft_ctr as corrective_action
from edw_rpt_copa_customergp_agg custgp,itg_custpl_excp_ctrl exc,itg_mds_custgp_portfolio_mapping portf		--// from rg_edw.edw_rpt_copa_customergp_agg custgp,rg_itg.itg_custpl_excp_ctrl exc,rg_itg.itg_mds_custgp_portfolio_mapping portf
where (custgp.ctry_nm = exc.ctry_nm and 'regprodseg' = exc.prodkey) and		--// where (custgp.ctry_nm = exc.ctry_nm and 'regprodseg' = exc.prodkey) and
(custgp.prft_ctr != 'not available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and		--// (custgp.prft_ctr != 'not available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and
(custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)		--// (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)
union all
select distinct 'incomplete market product portfolio' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for '||custgp.prft_ctr as error_desc,		--//        exc.error_desc||' for '||custgp.prft_ctr as error_desc,
       exc.corrective_action ||' for '||custgp.prft_ctr as corrective_action		--//        exc.corrective_action ||' for '||custgp.prft_ctr as corrective_action
from edw_rpt_copa_customergp_agg custgp,itg_custpl_excp_ctrl exc,itg_mds_custgp_portfolio_mapping portf		--// from rg_edw.edw_rpt_copa_customergp_agg custgp,rg_itg.itg_custpl_excp_ctrl exc,rg_itg.itg_mds_custgp_portfolio_mapping portf

where (custgp.ctry_nm = exc.ctry_nm and 'locprodseg' = exc.prodkey) and		--// where (custgp.ctry_nm = exc.ctry_nm and 'locprodseg' = exc.prodkey) and
(custgp.prft_ctr != 'not available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and		--// (custgp.prft_ctr != 'not available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and
(custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)		--// (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)
union all
select 'missing product portfolio' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       exc.error_desc||' for '||custgp.prft_ctr as error_desc,		--//        exc.error_desc||' for '||custgp.prft_ctr as error_desc,
       exc.corrective_action ||' for '||custgp.prft_ctr as corrective_action		--//        exc.corrective_action ||' for '||custgp.prft_ctr as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
join itg_custpl_excp_ctrl exc on (custgp.ctry_nm = exc.ctry_nm and 'misprodseg' = exc.prodkey)		--// join rg_itg.itg_custpl_excp_ctrl exc on (custgp.ctry_nm = exc.ctry_nm and 'misprodseg' = exc.prodkey)
left join itg_mds_custgp_portfolio_mapping portf on (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)		--// left join rg_itg.itg_mds_custgp_portfolio_mapping portf on (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)
where (custgp.prft_ctr != 'not available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and		--// where (custgp.prft_ctr != 'not available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and
(portf.prft_ctr = '' or portf.prft_ctr is null)		--// (portf.prft_ctr = '' or portf.prft_ctr is null)
union all
select distinct 'missing pre-apsc cost' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.period as period,		--//        custgp.period as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.profit_cntr as profit_center,		--//        custgp.profit_cntr as profit_center,
       'missing pre-apsc cost'||' for '||custgp.matl_num || ' for market '||custgp.ctry_nm as error_desc,		--//        'missing pre-apsc cost'||' for '||custgp.matl_num || ' for market '||custgp.ctry_nm as error_desc,
       'please add an entry for material ' ||custgp.matl_num || ' for market '|| custgp.ctry_nm ||' in mds table ap_pre_apsc_master'		--//        'please add an entry for material ' ||custgp.matl_num || ' for market '|| custgp.ctry_nm ||' in mds table ap_pre_apsc_master'
	   as corrective_action
from
(
select
calccogs.fisc_yr,
calccogs.period,
calccogs.ctry_group as ctry_nm,
calccogs.matl_num,
profit_cntr
from
(select
fisc_yr,
period,
cmp.ctry_group as ctry_group,
matl_num,
profit_cntr from
edw_custgp_cogs_automation cogs		--// from rg_edw.edw_custgp_cogs_automation cogs
left join edw_company_dim cmp on cogs.co_cd = cmp.co_cd		--// left join rg_edw.edw_company_dim cmp on cogs.co_cd = cmp.co_cd
where pre_apsc_cper_pc is null
and nts_volume != 0
and (matl_num is not null and matl_num != ''))calccogs
left join ( select market_code,materialnumber,materialdescription,valid_from,
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from,
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                  pre_apsc_per_pc
            from itg_mds_pre_apsc_master		--//             from rg_itg.itg_mds_pre_apsc_master
          --where -- materialnumber = '6127076' and
            --market_code ='korea'
         )  base

        order by market_code, materialnumber, valid_from
       ) preapsc on preapsc.materialnumber = calccogs.matl_num		--//        ) preapsc on preapsc.materialnumber = calccogs.matl_num
        and preapsc.market_code = calccogs.ctry_group		--//         and preapsc.market_code = calccogs.ctry_group
        and calccogs.period >= preapsc.valid_from		--//         and calccogs.period >= preapsc.valid_from
        and calccogs.period < preapsc.valid_to		--//         and calccogs.period < preapsc.valid_to
where (preapsc.materialnumber is null or preapsc.materialnumber = ' '))custgp		--// where (preapsc.materialnumber is null or preapsc.materialnumber = ' '))custgp
union all
select distinct 'missing pre-apsc cost' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.period as period,		--//        custgp.period as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.profit_cntr as profit_center,		--//        custgp.profit_cntr as profit_center,
       'missing pre-apsc cost'||' for '||custgp.matl_num || ' for market '||custgp.ctry_nm as error_desc,		--//        'missing pre-apsc cost'||' for '||custgp.matl_num || ' for market '||custgp.ctry_nm as error_desc,
       'please update pre-apsc cost for material ' ||custgp.matl_num || ' for market '|| custgp.ctry_nm ||' in mds table ap_pre_apsc_master'		--//        'please update pre-apsc cost for material ' ||custgp.matl_num || ' for market '|| custgp.ctry_nm ||' in mds table ap_pre_apsc_master'
	   as corrective_action
from
(
select
calccogs.fisc_yr,
calccogs.period,
calccogs.ctry_group as ctry_nm,
calccogs.matl_num,
profit_cntr
from
(select
fisc_yr,
period,
cmp.ctry_group as ctry_group,
matl_num,profit_cntr
from edw_custgp_cogs_automation cogs		--// from rg_edw.edw_custgp_cogs_automation cogs
left join edw_company_dim cmp on cogs.co_cd = cmp.co_cd		--// left join rg_edw.edw_company_dim cmp on cogs.co_cd = cmp.co_cd
where pre_apsc_cper_pc is null
and nts_volume != 0
and (matl_num is not null and matl_num != ''))calccogs
left join ( select market_code,materialnumber,materialdescription,valid_from,
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from,
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                  pre_apsc_per_pc
            from itg_mds_pre_apsc_master		--//             from rg_itg.itg_mds_pre_apsc_master
          --where -- materialnumber = '6127076' and
            --market_code ='korea'
         )  base

        order by market_code, materialnumber, valid_from
       ) preapsc on preapsc.materialnumber = calccogs.matl_num		--//        ) preapsc on preapsc.materialnumber = calccogs.matl_num
        and preapsc.market_code = calccogs.ctry_group		--//         and preapsc.market_code = calccogs.ctry_group
        and calccogs.period >= preapsc.valid_from		--//         and calccogs.period >= preapsc.valid_from
        and calccogs.period < preapsc.valid_to		--//         and calccogs.period < preapsc.valid_to
where (preapsc.materialnumber is not null or preapsc.materialnumber != ' ') and (pre_apsc_cper_pc is null or  pre_apsc_cper_pc = 0))custgp		--// where (preapsc.materialnumber is not null or preapsc.materialnumber != ' ') and (pre_apsc_cper_pc is null or  pre_apsc_cper_pc = 0))custgp
union all
select distinct 'unassigned customer number in copa' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       'not applicable' as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       'blank customer number in bw copa extract' as error_desc,
       'please update correct customer number in bw copa extract to dna' as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
where cust_num is null or cust_num = '' or cust_num = 'not available'
union all
select distinct 'unassigned material number in copa' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       'not applicable' as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       'blank material number in bw copa extract' as error_desc,
       'please update correct customer number in bw copa extract to dna' as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
where matl_num is null or matl_num = '' or matl_num = 'not available'
union all
select distinct 'unassigned profit center in copa' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       custgp.ctry_nm as market,		--//        custgp.ctry_nm as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       'not applicable' as profit_center,
       'blank profit center in bw copa extract' as error_desc,
       'please update correct profit center in bw copa extract to dna' as corrective_action
from edw_rpt_copa_customergp_agg custgp		--// from rg_edw.edw_rpt_copa_customergp_agg custgp
where prft_ctr is null or prft_ctr = '' or prft_ctr = 'not available'
union all
select distinct 'incomplete copa record in dna' as error_category,
       custgp.fisc_yr as year,		--//        custgp.fisc_yr as year,
       custgp.fisc_yr_per as period,		--//        custgp.fisc_yr_per as period,
       cmp.ctry_group as market,		--//        cmp.ctry_group as market,
       custgp.cust_num as "customer code",		--//        custgp.cust_num as "customer code",
       custgp.matl_num as "material code",		--//        custgp.matl_num as "material code",
       custgp.prft_ctr as profit_center,		--//        custgp.prft_ctr as profit_center,
       'blank/empty sales org in copa record' as error_desc,
       'please update the necessary sales org in bw copa extract to dna' as corrective_action
from edw_copa_trans_fact custgp,edw_company_dim cmp,itg_custgp_cogs_fg_control fgctl		--// from rg_edw.edw_copa_trans_fact custgp,rg_edw.edw_company_dim cmp,rg_itg.itg_custgp_cogs_fg_control fgctl
where custgp.acct_hier_shrt_desc in ('nts','fg') and		--// where custgp.acct_hier_shrt_desc in ('nts','fg') and
(custgp.co_cd = cmp.co_cd) and		--// (custgp.co_cd = cmp.co_cd) and
(custgp.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and		--// (custgp.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and
													   custgp.co_cd = fgctl.co_cd and		--// 													   custgp.co_cd = fgctl.co_cd and
													   custgp.fisc_yr_per >= fgctl.valid_from and		--// 													   custgp.fisc_yr_per >= fgctl.valid_from and
													   custgp.fisc_yr_per < fgctl.valid_to and		--// 													   custgp.fisc_yr_per < fgctl.valid_to and
													   case when custgp.acct_hier_shrt_desc = 'fg'		--// 													   case when custgp.acct_hier_shrt_desc = 'fg'
													        then ltrim(custgp.acct_num,'0') = fgctl.gl_acct_num		--// 													        then ltrim(custgp.acct_num,'0') = fgctl.gl_acct_num
															when custgp.acct_hier_shrt_desc = 'nts'		--// 															when custgp.acct_hier_shrt_desc = 'nts'
													        then '0' = fgctl.gl_acct_num end and		--// 													        then '0' = fgctl.gl_acct_num end and
													   fgctl.active = 'y'	)	and		--// 													   fgctl.active = 'y'	)	and
(sls_org is null or sls_org = '' or sls_org = 'not available')  
)
select * from final