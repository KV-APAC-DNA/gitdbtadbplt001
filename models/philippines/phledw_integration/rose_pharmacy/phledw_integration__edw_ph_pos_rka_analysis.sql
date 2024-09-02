with ph_pos_rka_rose_pharma as 
 (
    select * from {{ ref('phlitg_integration__itg_ph_pos_rka_rose_pharma') }}
 ),
ph_rosepharma_customers as 
(
    select * from {{ ref('phlitg_integration__itg_ph_pos_mds_rka_customers') }}
),
ph_rosepharma_products as 
(
     select * from {{ ref('phlitg_integration__itg_ph_pos_rka_product_mapping') }}
),
price_list as 
(
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
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
epmad as(
    select distinct item_cd,
			promo_reg_ind,
			hero_sku_ind,
			promo_strt_period,
			npi_strt_period
		from itg_mds_ph_lav_product
		where active = 'Y'
),
veocd as(
    select *
		from edw_vw_ph_customer_dim
		where sap_cntry_cd = 'PH'
),

transformed as 
(
select 
    'PH' AS cntry_cd,
    'Philippines' AS cntry_nm,
    pos.jj_year,
    pos.jj_month,
    pos.jj_month_id,
    time_dim.qrtr_no as jj_qtr,
    null as cntry_nm,
    cust.Code as cust_cd,
    cust.brnch_cd as cust_brnch_cd,
    cust.brnch_nm as mt_cust_brnch_nm,
    prod.sap_item_cd as item_cd,
    prod.sap_item_desc as mt_item_nm,
    split_part(prod.parent_cust_cd,'-',1) as sold_to,
    null as sold_to_nm,
    null as region,
    null as chnl_desc,
    null as sub_chnl_cd,
    null as sub_chnl_desc,
    split_part(cust.Code,'-',1) as parent_customer_cd,
    cust.cust_cd as parent_customer,
   ltrim(veomd.sap_matl_num, '0') as sku,
	veomd.sap_mat_desc as sku_desc,
    POS.qty,
    coalesce(prod.jnj_pc_per_cust_unit,1) as jnj_pc_per_cust_unit,
    price.Lst_Price_Unit as ListPriceUnit,
    (pos.qty/prod.jnj_pc_per_cust_unit) as pos_qty,
    (pos_qty*ListPriceUnit) as pos_gts
     ,null as jj_vat_amt
,veomd.sap_prod_sgmt_cd as sap_prod_sgmt_cd
,veomd.sap_base_uom_cd as sap_base_uom_cd
,veomd.gph_prod_put_up_cd as global_prod_put_up_cd
,veomd.sap_prod_sgmt_desc as sap_prod_sgmt_desc
,veomd.sap_prchse_uom_cd as sap_prchse_uom_cd
,veomd.gph_prod_put_up_desc as global_put_up_desc
,null as jj_nts
,veomd.sap_base_prod_cd as sap_base_prod_cd
,gph_prod_sub_brnd as global_prod_sub_brand
,null as account_grp
,veomd.sap_base_prod_desc as sap_base_prod_desc
,veomd.gph_prod_needstate as global_prod_need_state
,'MODERN TRADE' as trade_type
,veomd.sap_mega_brnd_cd as sap_mega_brnd_cd
,veomd.gph_prod_ctgry as global_prod_category
,null as sls_grp_desc
,veomd.sap_mega_brnd_desc as sap_mega_brnd_desc
,veomd.gph_prod_subctgry as global_prod_subcategory
,null as sap_state_cd
,veomd.sap_brnd_cd as sap_brnd_cd
,veomd.gph_prod_sgmnt as global_prod_segment
,null as sap_sls_org
,veomd.sap_brnd_desc as sap_brnd_desc
,veomd.gph_prod_subsgmnt as global_prod_subsegment
,null as sap_cmp_id
,null as sap_vrnt_cd
,veomd.gph_prod_size as global_prod_size
,veomd.sap_vrnt_desc as sap_cntry_cd
,veomd.sap_vrnt_desc as sap_vrnt_desc
,veomd.gph_prod_size_uom as global_prod_size_uom
,null as sap_cntry_nm
,veomd.sap_put_up_cd as sap_put_up_cd
,case
		WHEN UPPER(EPMAD.PROMO_REG_IND) = 'REG'
			THEN 'Y'
		ELSE 'N'
		END AS IS_REG
,null as sap_addr
,veomd.sap_put_up_desc as sap_put_up_desc
,null as is_promo
,null as sap_region
,veomd.sap_grp_frnchse_cd as sap_grp_frnchse_cd
,null as local_mat_promo_strt_period
,null as sap_city
,veomd.sap_grp_frnchse_desc as sap_grp_frnchse_desc
,null as is_npi
,null as sap_post_cd
,null as sap_frnchse_cd
,	CASE
		WHEN UPPER(EPMAD.HERO_SKU_IND) = 'Y'
			THEN 'HERO'
		ELSE 'NA'
		END AS IS_HERO
,null as sap_chnl_cd
,null as sap_frnchse_desc
,null as is_mcl
,null as sap_chnl_desc
,null as sap_prod_frnchse_cd
,null as local_mat_npi_strt_period
,null as sap_sls_office_cd
,null as sap_prod_frnchse_desc
,null as pka_productkey
,null as sap_sls_office_desc
,null as sap_prod_mjr_cd
,null as region_cd
,null as sap_sls_grp_cd
,null as sap_prod_mjr_desc
,null as region_nm
,null as sap_sls_grp_desc
,null as sap_prod_mnr_cd
,cust.prov_cd as prov_cd
,cust.prov_nm as prov_nm
,null as sap_curr_cd
,null as sap_prod_mnr_desc
,null as gch_region
,null as sap_prod_hier_cd
,cust.mncplty_cd as mncplty_cd
,cust.mncplty_nm as mncplty_nm
,null as gch_cluster
,null as sap_prod_hier_desc
,null as gch_subcluster
,null as global_mat_region
,null as city_cd
,null as gch_market
,null as global_prod_franchise
,null as city_nm
,null as gch_retail_banner
,veomd.gph_prod_brnd as global_prod_brand
,null as ae_nm
,veomd.sap_mat_type_cd as sap_mat_type_cd
,veomd.gph_prod_vrnt as global_prod_variant
,null as ash_no
,veomd.sap_mat_type_desc as sap_mat_type_desc
,null as ash_nm
,null as pms_nm
    from ph_pos_rka_rose_pharma as pos
     left  join edw_vw_os_time_dim time_dim on (pos.jj_month_id=time_dim.mnth_id)
     left   join ph_rosepharma_products prod on (pos.sku=prod.item_cd)
     left  join ph_rosepharma_customers cust on (pos.branch_code=cust.brnch_cd)
     left   join price_list price on (prod.sap_item_cd=price.item_cd and prod.mnth_id=price.jj_mnth_id and price.active='Y') 
     left join veomd on  (upper(ltrim(veomd.sap_matl_num, 0)) = prod.sap_item_cd)
     left join epmad on (upper(trim(epmad.item_cd)) = prod.sap_item_cd)
  ),
final as 
(
select 
*
from transformed

)
select * from final 