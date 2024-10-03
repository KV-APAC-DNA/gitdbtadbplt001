with ph_pos_rka_rose_pharma as 
 (
    select * from {{ ref('phlitg_integration__itg_ph_pos_rka_rose_pharma') }}
 ),
ph_rosepharma_customers as 
(
    select * from {{ ref('phlitg_integration__itg_ph_pos_rka_mds_customers') }}
),
ph_rosepharma_products as 
(
     select * from {{ ref('phlitg_integration__itg_ph_pos_rka_mds_product_mapping') }}
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
itg_mds_ph_lav_product as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
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
   'RKA007' as parent_customer_cd,
    'ROSE PHARMACY, INC.' as parent_customer,
   ltrim(veomd.sap_matl_num, '0') as sku,
	veomd.sap_mat_desc as sku_desc,
    POS.qty::integer as qty,
    --(pos.qty*jj_item_prc_per_pc) ::decimal(38,3) as pos_gts,
    null as pos_gts,
    null as pos_item_prc,
    null as pos_tax,
    null as pos_nts,
    coalesce(prod.jnj_pc_per_cust_unit,1) as conv_factor,
    (pos.qty/prod.jnj_pc_per_cust_unit) ::decimal(38,3) as jj_qty_pc,
    (price.Lst_Price_Unit)::decimal(38,3) as jj_item_prc_per_pc,
    (cast(pos.qty as numeric(20,4))*cast(prod.jnj_pc_per_cust_unit as numeric(20,4)))*cast(price.lst_price_unit as numeric(20,4)) as jj_gts,
    ((cast(pos.qty as numeric(20,4))*cast(prod.jnj_pc_per_cust_unit as numeric(20,4)))*cast(price.lst_price_unit as numeric(20,4)))*(12.0 / 112.0) as jj_vat_amt, 

((cast(pos.qty as numeric(20,4))*cast(prod.jnj_pc_per_cust_unit as numeric(20,4)))*cast(price.lst_price_unit as numeric(20,4)))*(100.0 / 112.0) as jj_nts

,veomd.pka_productkey
,veomd.sap_prod_sgmt_cd as sap_prod_sgmt_cd
,veomd.sap_base_uom_cd as sap_base_uom_cd
,veomd.gph_prod_put_up_cd as global_prod_put_up_cd
,veomd.sap_prod_sgmt_desc as sap_prod_sgmt_desc
,veomd.sap_prchse_uom_cd as sap_prchse_uom_cd
,veomd.gph_prod_put_up_desc as global_put_up_desc
,veomd.sap_base_prod_cd as sap_base_prod_cd
,gph_prod_sub_brnd as global_prod_sub_brand
,'ROSE PHARMACY, INC.' as account_grp
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
,veomd.sap_vrnt_cd as sap_vrnt_cd
,veomd.gph_prod_size as global_prod_size
,veocd.sap_cntry_cd as sap_cntry_cd
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
,veomd.sap_prod_frnchse_cd as sap_prod_frnchse_cd
,null as local_mat_npi_strt_period
,null as sap_sls_office_cd
,veomd.sap_prod_frnchse_desc as sap_prod_frnchse_desc
,null as sap_sls_office_desc
,veomd.sap_prod_mjr_cd as sap_prod_mjr_cd
,null as region_cd
,null as sap_sls_grp_cd
,veomd.sap_prod_mjr_desc as sap_prod_mjr_desc
,null as region_nm
,null as sap_sls_grp_desc
,veomd.sap_prod_mnr_cd as sap_prod_mnr_cd
,cust.prov_cd as prov_cd
,cust.prov_nm as prov_nm
,null as sap_curr_cd
,veomd.sap_prod_mnr_desc as sap_prod_mnr_desc
,'APAC' as gch_region
,veomd.sap_prod_hier_cd as sap_prod_hier_cd
,cust.mncplty_cd as mncplty_cd
,cust.mncplty_nm as mncplty_nm
,null as gch_cluster
,veomd as sap_prod_hier_desc
,null as gch_subcluster
,null as global_mat_region
,null as city_cd
,'Philippines' as gch_market
,veomd.gph_prod_frnchse as global_prod_franchise
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
     left  join (select distinct mnth_id,qrtr_no from EDW_VW_OS_TIME_DIM group by 1,2) time_dim on (pos.jj_month_id=time_dim.mnth_id)
     left   join ph_rosepharma_products prod on (pos.sku=prod.item_cd and pos.jj_month_id=prod.mnth_id)
     left  join ph_rosepharma_customers cust on (pos.branch_code=cust.brnch_cd)
     left   join price_list price on (prod.sap_item_cd=price.item_cd and prod.mnth_id=price.jj_mnth_id and price.active='Y') 
     left join veomd on  (upper(ltrim(veomd.sap_matl_num, 0)) = upper(ltrim(prod.sap_item_cd,0)))
     left join epmad on (upper(trim(epmad.item_cd)) = upper(ltrim(prod.sap_item_cd,0)))
    
  ),
final as 
(
 
select 
        jj_year::number(18,0) as jj_year,
        jj_qtr::varchar(14) as jj_qtr,
        jj_mnth_id::varchar(30) as jj_mnth_id,
        jj_mnth_no::number(18,0) as jj_mnth_no,
        cntry_nm::varchar(11) as cntry_nm,
        cust_cd::varchar(50) as cust_cd,
        cust_brnch_cd::varchar(300) as cust_brnch_cd,
        mt_cust_brnch_nm::varchar(300) as mt_cust_brnch_nm,
        region_cd::varchar(255) as region_cd,
        region_nm::varchar(255) as region_nm,
        prov_cd::varchar(255) as prov_cd,
        prov_nm::varchar(255) as prov_nm,
        mncplty_cd::varchar(255) as mncplty_cd,
        mncplty_nm::varchar(255) as mncplty_nm,
        city_cd::varchar(255) as city_cd,
        city_nm::varchar(255) as city_nm,
        ae_nm::varchar(255) as ae_nm,
        ash_no::varchar(255) as ash_no,
        ash_nm::varchar(255) as ash_nm,
        pms_nm::varchar(255) as pms_nm,
        item_cd::varchar(300) as item_cd,
        mt_item_nm::varchar(500) as mt_item_nm,
        sold_to::varchar(30) as sold_to,
        sold_to_nm::varchar(100) as sold_to_nm,
        region::varchar(150) as region,
        chnl_cd::varchar(50) as chnl_cd,
        chnl_desc::varchar(255) as chnl_desc,
        sub_chnl_cd::varchar(50) as sub_chnl_cd,
        sub_chnl_desc::varchar(255) as sub_chnl_desc,
        parent_customer_cd::varchar(50) as parent_customer_cd,
        parent_customer::varchar(255) as parent_customer,
        account_grp::varchar(255) as account_grp,
        trade_type::varchar(12) as trade_type,
        sls_grp_desc::varchar(255) as sls_grp_desc,
        sap_state_cd::varchar(150) as sap_state_cd,
        sap_sls_org::varchar(4) as sap_sls_org,
        sap_cmp_id::varchar(6) as sap_cmp_id,
        sap_cntry_cd::varchar(3) as sap_cntry_cd,
        sap_cntry_nm::varchar(40) as sap_cntry_nm,
        sap_addr::varchar(150) as sap_addr,
        sap_region::varchar(150) as sap_region,
        sap_city::varchar(150) as sap_city,
        sap_post_cd::varchar(150) as sap_post_cd,
        sap_chnl_cd::varchar(2) as sap_chnl_cd,
        sap_chnl_desc::varchar(20) as sap_chnl_desc,
        sap_sls_office_cd::varchar(4) as sap_sls_office_cd,
        sap_sls_office_desc::varchar(40) as sap_sls_office_desc,
        sap_sls_grp_cd::varchar(3) as sap_sls_grp_cd,
        sap_sls_grp_desc::varchar(40) as sap_sls_grp_desc,
        sap_curr_cd::varchar(5) as sap_curr_cd,
        gch_region::varchar(50) as gch_region,
        gch_cluster::varchar(50) as gch_cluster,
        gch_subcluster::varchar(50) as gch_subcluster,
        gch_market::varchar(50) as gch_market,
        gch_retail_banner::varchar(50) as gch_retail_banner,
        sku::varchar(40) as sku,
        sku_desc::varchar(100) as sku_desc,
        sap_mat_type_cd::varchar(10) as sap_mat_type_cd,
        sap_mat_type_desc::varchar(40) as sap_mat_type_desc,
        sap_base_uom_cd::varchar(10) as sap_base_uom_cd,
        sap_prchse_uom_cd::varchar(10) as sap_prchse_uom_cd,
        sap_prod_sgmt_cd::varchar(18) as sap_prod_sgmt_cd,
        sap_prod_sgmt_desc::varchar(100) as sap_prod_sgmt_desc,
        sap_base_prod_cd::varchar(10) as sap_base_prod_cd,
        sap_base_prod_desc::varchar(100) as sap_base_prod_desc,
        sap_mega_brnd_cd::varchar(10) as sap_mega_brnd_cd,
        sap_mega_brnd_desc::varchar(100) as sap_mega_brnd_desc,
        sap_brnd_cd::varchar(10) as sap_brnd_cd,
        sap_brnd_desc::varchar(100) as sap_brnd_desc,
        sap_vrnt_cd::varchar(10) as sap_vrnt_cd,
        sap_vrnt_desc::varchar(100) as sap_vrnt_desc,
        sap_put_up_cd::varchar(10) as sap_put_up_cd,
        sap_put_up_desc::varchar(100) as sap_put_up_desc,
        sap_grp_frnchse_cd::varchar(18) as sap_grp_frnchse_cd,
        sap_grp_frnchse_desc::varchar(100) as sap_grp_frnchse_desc,
        sap_frnchse_cd::varchar(18) as sap_frnchse_cd,
        sap_frnchse_desc::varchar(100) as sap_frnchse_desc,
        sap_prod_frnchse_cd::varchar(18) as sap_prod_frnchse_cd,
        sap_prod_frnchse_desc::varchar(100) as sap_prod_frnchse_desc,
        sap_prod_mjr_cd::varchar(18) as sap_prod_mjr_cd,
        sap_prod_mjr_desc::varchar(100) as sap_prod_mjr_desc,
        sap_prod_mnr_cd::varchar(18) as sap_prod_mnr_cd,
        sap_prod_mnr_desc::varchar(100) as sap_prod_mnr_desc,
        sap_prod_hier_cd::varchar(18) as sap_prod_hier_cd,
        sap_prod_hier_desc::varchar(100) as sap_prod_hier_desc,
        global_mat_region::varchar(50) as global_mat_region,
        global_prod_franchise::varchar(30) as global_prod_franchise,
        global_prod_brand::varchar(30) as global_prod_brand,
        global_prod_variant::varchar(100) as global_prod_variant,
        global_prod_put_up_cd::varchar(10) as global_prod_put_up_cd,
        global_put_up_desc::varchar(100) as global_put_up_desc,
        global_prod_sub_brand::varchar(100) as global_prod_sub_brand,
        global_prod_need_state::varchar(50) as global_prod_need_state,
        global_prod_category::varchar(50) as global_prod_category,
        global_prod_subcategory::varchar(50) as global_prod_subcategory,
        global_prod_segment::varchar(50) as global_prod_segment,
        global_prod_subsegment::varchar(100) as global_prod_subsegment,
        global_prod_size::varchar(20) as global_prod_size,
        global_prod_size_uom::varchar(20) as global_prod_size_uom,
        is_reg::varchar(1) as is_reg,
        is_promo::varchar(1) as is_promo,
        local_mat_promo_strt_period::varchar(50) as local_mat_promo_strt_period,
        is_npi::varchar(1) as is_npi,
        is_hero::varchar(4) as is_hero,
        is_mcl::varchar(1) as is_mcl,
        local_mat_npi_strt_period::varchar(50) as local_mat_npi_strt_period,
        pos_qty::float as pos_qty,
        pos_gts::float as pos_gts,
        pos_item_prc::float as pos_item_prc,
        pos_tax::float as pos_tax,
        pos_nts::float as pos_nts,
        conv_factor::number(20,4) as conv_factor,
        jj_qty_pc::number(22,6) as jj_qty_pc,
        jj_item_prc_per_pc::number(20,4) as jj_item_prc_per_pc,
        jj_gts::number(37,10) as jj_gts,
        jj_vat_amt::number(20,4) as jj_vat_amt,
        jj_nts::number(37,10) as jj_nts,
        pka_productkey::varchar(68) as pka_productkey
    
from transformed

)
select * from final 