with edw_investment_fact as (
    select * from dev_dna_core.aspedw_integration.edw_investment_fact
),
itg_total_investment_brand_map as (
    select * from dev_dna_core.aspitg_integration.itg_total_investment_brand_map
),
itg_mds_rg_total_investment_ppm as (
    select * from dev_dna_core.aspitg_integration.itg_mds_rg_total_investment_ppm
),
trans as
(
   select
    eifb.posting_fiscal_year,
	eifb.posting_fiscal_period_number,
	eifb.version_group_code,
	eifb.mrc_code,
	eifb.mrc_counrty_code,
	eifb.mrc_name,
	eifb.cluster_name,
	eifb.country_name,
	eifb.bravo_product_cd,
	eifb.bravo_product_name,
	eifb.strng_product_lvl_02_desc,
	eifb.strng_product_lvl_03_desc,
	eifb.franchise,
	eifb.stronghold,
	eifb.gl_account_code,
	eifb.gl_account_desc,
	eifb.equalization_flag,
	eifb.description_level_00,
	eifb.description_level_01,
	eifb.description_level_02,
	eifb.usdf_mtd_amount,
	eifb.kpi,
	eifb.brand,
	rtippm.product_minor,
	rtippm.ppm_role
FROM (
	(
		SELECT eif.posting_fiscal_year,
			eif.posting_fiscal_period_number,
			eif.version_group_code,
			eif.mrc_code,
			eif.mrc_counrty_code,
			eif.mrc_name,
			eif.cluster_name,
			eif.country_name,
			eif.bravo_product_cd,
			eif.bravo_product_name,
			eif.strng_product_lvl_02_desc,
			eif.strng_product_lvl_03_desc,
			eif.franchise,
			eif.stronghold,
			eif.gl_account_code,
			eif.gl_account_desc,
			eif.equalization_flag,
			eif.description_level_00,
			eif.description_level_01,
			eif.description_level_02,
			eif.usdf_mtd_amount,
			eif.kpi,
			rtibm.brand
		FROM (
			edw_investment_fact eif LEFT JOIN (
				SELECT DISTINCT itg_total_investment_brand_map.brand,
					itg_total_investment_brand_map.bravo_product_code
				FROM itg_total_investment_brand_map
				) rtibm ON ((trim(upper((rtibm.bravo_product_code)::TEXT)) = trim(upper((eif.bravo_product_cd)::TEXT))))
			)
		) eifb LEFT JOIN (
		SELECT DISTINCT rtippm.brand,
			rtippm.product_minor,
			rtippm.mrc_code,
			rtippm.mrc_name,
			rtippm.ppm_role
		FROM itg_mds_rg_total_investment_ppm rtippm
		) rtippm ON (
			(
				(
					(trim(upper((eifb.mrc_code)::TEXT)) = trim(upper((rtippm.mrc_code)::TEXT)))
					AND (trim(upper((COALESCE(eifb.brand, 'NA'::CHARACTER VARYING))::TEXT)) = trim(upper((COALESCE(rtippm.brand, 'NA'::CHARACTER VARYING))::TEXT)))
					)
				AND (trim(upper((((eifb.bravo_product_cd)::TEXT || ' - '::TEXT) || (eifb.bravo_product_name)::TEXT))) = trim(upper((rtippm.product_minor)::TEXT)))
				)
			)
	) 
),
final as
(
    select
    posting_fiscal_year::number(38,0) as posting_fiscal_year,
	posting_fiscal_period_number::number(38,0) as posting_fiscal_period_number,
	version_group_code::varchar(50) as version_group_code,
	mrc_code::varchar(50) as mrc_code,
	mrc_counrty_code::varchar(255) as mrc_counrty_code,
	mrc_name::varchar(255) as mrc_name,
	cluster_name::varchar(255) as cluster_name,
	country_name::varchar(255) as country_name,
	bravo_product_cd::varchar(255) as bravo_product_cd,
	bravo_product_name::varchar(255) as bravo_product_name,
	strng_product_lvl_02_desc::varchar(255) as strng_product_lvl_02_desc,
	strng_product_lvl_03_desc::varchar(255) as strng_product_lvl_03_desc,
	franchise::varchar(255) as franchise,
	stronghold::varchar(255) as stronghold,
	gl_account_code::varchar(255) as gl_account_code,
	gl_account_desc::varchar(255) as gl_account_desc,
	equalization_flag::varchar(255) as equalization_flag,
	description_level_00::varchar(255) as description_level_00,
	description_level_01::varchar(255) as description_level_01,
	description_level_02::varchar(255) as description_level_02,
	usdf_mtd_amount::number(38,0) as usdf_mtd_amount,
	kpi::varchar(255) as kpi,
	brand::varchar(200) as brand,
	product_minor::varchar(200) as product_minor,
	ppm_role::varchar(200) as ppm_role
    from trans
)
select * from final