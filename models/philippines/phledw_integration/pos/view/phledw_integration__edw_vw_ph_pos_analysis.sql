-- get mnth_id for cartetian join
{%- call statement('get_mnth_id', fetch_result=True) -%}
        select mnth_id
	    from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
	    where cal_date::date = date(current_timestamp())
{%- endcall -%}

{%- set mnth_id = load_result('get_mnth_id')['data'][0][0] %}

with itg_mds_ph_pos_pricelist as(
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_mds_ph_lav_product as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
edw_vw_ph_pos_sales_fact as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_sales_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_ph_ref_pos_primary_sold_to as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_pos_primary_sold_to') }}
),
edw_vw_ph_pos_customer_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_customer_dim') }}
),
edw_vw_ph_pos_material_dim as (
    select * from {{ ref('phledw_integration__edw_vw_ph_pos_material_dim') }}
),
itg_mds_ph_pos_customers as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_customers') }}
),
edw_mv_ph_customer_dim as (
    select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_ph_material_dim as(
    select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
edw_vw_ph_customer_dim as(
    select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
edw_product_key_attributes as(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),

epp2 as(
    	select status,
			item_cd,
			min(jj_mnth_id) as launch_period,
			min(to_char(add_months(to_date(
								concat(
									jj_mnth_id,
									'01'
									),'yyyymmdd'
								), 11), 'YYYYMM')) as end_period
		from itg_mds_ph_pos_pricelist
		where status = '**'
			and active = 'Y'
		group by status,
			item_cd
),
epmad as(
    select item_cd,
			promo_reg_ind,
			hero_sku_ind,
			promo_strt_period,
			npi_strt_period
		from itg_mds_ph_lav_product
		where active = 'Y'
),
veposf as(
    select b."year",
			b.qrtr,
			b.qrtr_no,
			b.mnth_no,
			a.cntry_cd,
			c.cntry_nm,
			a.jj_mnth_id,
			a.cust_cd,
			cust_brnch_cd,
			e.primary_soldto as sold_to,
			c.brnch_nm,
			c.region_cd,
			c.region_nm,
			c.prov_cd,
			c.prov_nm,
			c.mncplty_cd,
			c.mncplty_nm,
			c.city_cd,
			c.city_nm,
			f.ae_nm,
			f.ash_no,
			f.ash_nm,
			f.pms_nm,
			a.item_cd,
			d.item_nm,
			d.sap_item_cd,
			pos_qty,
			pos_gts,
			pos_item_prc,
			pos_tax,
			pos_nts,
			conv_factor,
			jj_qty_pc,
			jj_item_prc_per_pc,
			jj_gts,
			jj_vat_amt,
			jj_nts
		from (
			select *
			from edw_vw_ph_pos_sales_fact
			where cntry_cd = 'PH'
			) a,
			(
				select distinct "year",
					qrtr_no,
					qrtr,
					mnth_id,
					mnth_desc,
					mnth_no
				from edw_vw_os_time_dim
				) b,
			(
				select *
				from itg_mds_ph_ref_pos_primary_sold_to
				where active = 'Y'
				) e,
			(
				select *
				from edw_vw_ph_pos_customer_dim
				where cntry_cd = 'PH'
				) c,
			(
				select *
				from edw_vw_ph_pos_material_dim
				where cntry_cd = 'PH'
				) d,
			(
				select distinct cust_cd,
					brnch_cd,
					ae_nm,
					ash_no,
					ash_nm,
					pms_nm
				from itg_mds_ph_pos_customers
				where cust_cd in ('MDC', 'WAT')
					AND active = 'Y'
				) f
		where b.mnth_id = a.jj_mnth_id
			and c.brnch_cd(+) = a.cust_brnch_cd
			and c.cust_cd(+) = a.cust_cd
			and e.cust_cd(+) = a.cust_cd
			and ltrim(d.item_cd(+), '0') = ltrim(a.item_cd, '0')
			and d.jj_mnth_id(+) = a.jj_mnth_id
			and d.cust_cd(+) = a.cust_cd
			and f.cust_cd(+) = a.cust_cd
			and f.brnch_cd(+) = a.cust_brnch_cd
),
veomd as (
    select mat.*,
			prod.pka_productkey
		from edw_vw_ph_material_dim mat
		left join edw_product_key_attributes prod on ltrim(mat.sap_matl_num, '0') = ltrim(prod.matl_num, '0')
		where cntry_key = 'PH'
			AND upper(prod.ctry_nm) = 'PHILIPPINES'
),
veocd as(
    select *
		from edw_vw_ph_customer_dim
		where sap_cntry_cd = 'PH'
),


transformed as(
select veposf."year" as jj_year,
	veposf.qrtr as jj_qtr,
	veposf.jj_mnth_id,
	veposf.mnth_no as jj_mnth_no,
	veposf.cntry_nm as cntry_nm,
	veposf.cust_cd,
	veposf.cust_brnch_cd,
	veposf.brnch_nm as mt_cust_brnch_nm,
	veposf.region_cd,
	veposf.region_nm,
	veposf.prov_cd,
	veposf.prov_nm,
	veposf.mncplty_cd,
	veposf.mncplty_nm,
	veposf.city_cd,
	veposf.city_nm,
	veposf.ae_nm,
	veposf.ash_no,
	veposf.ash_nm,
	veposf.pms_nm,
	veposf.item_cd,
	veposf.item_nm as mt_item_nm,
	veposf.sold_to,
	veocd.sap_cust_nm as sold_to_nm,
	eocd.region,
	eocd.channel_cd as chnl_cd,
	eocd.channel_desc as chnl_desc,
	eocd.sub_chnl_cd as sub_chnl_cd,
	eocd.sub_chnl_desc as sub_chnl_desc,
	eocd.parent_cust_cd as parent_customer_cd,
	eocd.parent_cust_nm as parent_customer,
	eocd.rpt_grp_6_desc as account_grp,
	'MODERN TRADE' as trade_type,
	eocd.rpt_grp_2_desc as sls_grp_desc,
	veocd.sap_state_cd,
	veocd.sap_sls_org,
	veocd.sap_cmp_id,
	veocd.sap_cntry_cd,
	veocd.sap_cntry_nm,
	veocd.sap_addr,
	veocd.sap_region,
	veocd.sap_city,
	veocd.sap_post_cd,
	veocd.sap_chnl_cd,
	veocd.sap_chnl_desc,
	veocd.sap_sls_office_cd,
	veocd.sap_sls_office_desc,
	veocd.sap_sls_grp_cd,
	veocd.sap_sls_grp_desc,
	veocd.sap_curr_cd,
	veocd.gch_region,
	veocd.gch_cluster,
	veocd.gch_subcluster,
	veocd.gch_market,
	veocd.gch_retail_banner,
	ltrim(veomd.sap_matl_num, '0') as sku,
	veomd.sap_mat_desc as sku_desc,
	veomd.sap_mat_type_cd,
	veomd.sap_mat_type_desc,
	veomd.sap_base_uom_cd,
	veomd.sap_prchse_uom_cd,
	veomd.sap_prod_sgmt_cd,
	veomd.sap_prod_sgmt_desc,
	veomd.sap_base_prod_cd,
	veomd.sap_base_prod_desc,
	veomd.sap_mega_brnd_cd,
	veomd.sap_mega_brnd_desc,
	veomd.sap_brnd_cd,
	veomd.sap_brnd_desc,
	veomd.sap_vrnt_cd,
	veomd.sap_vrnt_desc,
	veomd.sap_put_up_cd,
	veomd.sap_put_up_desc,
	veomd.sap_grp_frnchse_cd,
	veomd.sap_grp_frnchse_desc,
	veomd.sap_frnchse_cd,
	veomd.sap_frnchse_desc,
	veomd.sap_prod_frnchse_cd,
	veomd.sap_prod_frnchse_desc,
	veomd.sap_prod_mjr_cd,
	veomd.sap_prod_mjr_desc,
	veomd.sap_prod_mnr_cd,
	veomd.sap_prod_mnr_desc,
	veomd.sap_prod_hier_cd,
	veomd.sap_prod_hier_desc,
	veomd.gph_region as global_mat_region,
	veomd.gph_prod_frnchse as global_prod_franchise,
	veomd.gph_prod_brnd as global_prod_brand,
	veomd.gph_prod_vrnt as global_prod_variant,
	veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
	veomd.gph_prod_put_up_desc as global_put_up_desc,
	veomd.gph_prod_sub_brnd as global_prod_sub_brand,
	veomd.gph_prod_needstate as global_prod_need_state,
	veomd.gph_prod_ctgry as global_prod_category,
	veomd.gph_prod_subctgry as global_prod_subcategory,
	veomd.gph_prod_sgmnt as global_prod_segment,
	veomd.gph_prod_subsgmnt as global_prod_subsegment,
	veomd.gph_prod_size as global_prod_size,
	veomd.gph_prod_size_uom as global_prod_size_uom,
	case
		WHEN UPPER(EPMAD.PROMO_REG_IND) = 'REG'
			THEN 'Y'
		ELSE 'N'
		END AS IS_REG,
	CASE
		WHEN UPPER(EPMAD.PROMO_REG_IND) = 'PROMO'
			THEN 'Y'
		ELSE 'N'
		END AS IS_PROMO,
	EPMAD.PROMO_STRT_PERIOD AS LOCAL_MAT_PROMO_STRT_PERIOD,
	CASE
		WHEN EPP2.STATUS = '**'
			AND (
				{{mnth_id}} BETWEEN EPP2.LAUNCH_PERIOD
					AND EPP2.END_PERIOD
				)
			THEN 'Y'
		ELSE 'N'
		END AS IS_NPI,
	CASE
		WHEN UPPER(EPMAD.HERO_SKU_IND) = 'Y'
			THEN 'HERO'
		ELSE 'NA'
		END AS IS_HERO,
	null as is_mcl,
	epp2.launch_period as local_mat_npi_strt_period,
	pos_qty,
	pos_gts,
	pos_item_prc,
	pos_tax,
	pos_nts,
	conv_factor,
	jj_qty_pc,
	jj_item_prc_per_pc,
	jj_gts,
	jj_vat_amt,
	jj_nts,
	veomd.pka_productkey
from 
    epp2,
    epmad,
    veposf,
    veomd, 
    veocd, 
    edw_mv_ph_customer_dim eocd
where upper(ltrim(veomd.sap_matl_num(+), 0)) = ltrim(veposf.sap_item_cd, '0')
	and upper(trim(eocd.cust_id(+))) = upper(trim(veposf.sold_to))
	and upper(ltrim(veocd.sap_cust_id(+), '0')) = upper(trim(veposf.sold_to))
	and upper(trim(epmad.item_cd(+))) = ltrim(veposf.sap_item_cd, '0')
	and upper(trim(epp2.item_cd(+))) = ltrim(veposf.sap_item_cd, '0')
)
select * from transformed