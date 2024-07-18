with itg_cnpc_re_msl_list
as (select * from {{ref('chnitg_integration__itg_cn_pc_re_msl_list')}}),

wks_china_personal_care_actuals
as (select * from {{ref('chnwks_integration__wks_china_personal_care_regional_sellout_actuals')}}),

edw_product_key_attributes 
as (select * from {{ ref('aspedw_integration__edw_generic_product_key_attributes') }}),

customer_hierarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_customer_hierarchy') }}
),
product_heirarchy as (
    select * from {{ ref('aspedw_integration__edw_generic_product_hierarchy') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),

edw_perfect_store_product_master
as (select * from {{source('chnedw_integration','edw_perfect_store_product_master')}}),

cnpc_rpt_retail_excellence_mdp
as
(
    select distinct q.*,
       com.cluster
from (select cast(target.fisc_yr as integer) as fisc_yr,
             cast(target.fisc_per as integer) as fisc_per,
             coalesce(actual.cntry_nm,target.prod_hier_l1,'NA') as market,
             coalesce(actual.data_src,target.data_src,'NA') as data_src,
			 coalesce(actual.soldto_code,target.soldto_code,'NA') as soldto_code,
             coalesce(actual.distributor_code,target.distributor_code,'NA') as distributor_code,
             coalesce(actual.distributor_name,target.distributor_name,'NA') as distributor_name,
             'not defined' as sell_out_channel,-----------------------------check
             coalesce(actual.store_type,target.store_type,'NA') as sell_out_re,-----------------------check
             coalesce(actual.store_type,target.store_type,'NA') as store_type,
             coalesce(actual.store_code,target.store_code,'NA') as store_code,
             coalesce(actual.store_name,target.store_name,'NA') as store_name,
             coalesce(actual.region,target.region,'NA') as region,
             coalesce(actual.zone,target.zone,'NA') as zone_name,
             coalesce(actual.city,target.city,'NA')as city,
             --coalesce(actual.pka_product_key_desc,target.product_code)as product_code,------------------------------------------------------keep as ean
			-- coalesce(actual.ean,target.product_code,'NA')as product_code,
			   coalesce(actual.product_code,target.product_code,'NA')as product_code,
			   coalesce(actual.ean,target.ean,'NA')as ean,
             --target.product_code as product_name,----------------------------- check
			 coalesce(actual.msl_product_desc,target.product_name,'NA') as product_name,
             target.prod_hier_l1 as prod_hier_l1,
             coalesce(upper(epspm.brand::text)::character varying,'NA') as prod_hier_l4,
             coalesce(upper(epspm.layer2::text)::character varying,'NA') as prod_hier_l5,
             coalesce(epspm.layer3,'NA') as prod_hier_l6,
             coalesce(epspm.product_name_en,'NA') as prod_hier_l9,
             coalesce(actual.mapped_sku_code,target.mapped_sku_code,'NA') as mapped_sku_cd,
             coalesce(actual.customer_segment_key,customer.cust_segmt_key,'NA') as customer_segment_key,
             coalesce(actual.customer_segment_description,customer.cust_segment_desc,'NA') as customer_segment_description,
             coalesce(actual.store_type,target.store_type,'NA') as retail_environment,
             coalesce(actual.sap_customer_channel_key,customer.sap_cust_chnl_key,'NA') as sap_customer_channel_key,
             coalesce(actual.sap_customer_channel_description,customer.sap_cust_chnl_desc,'NA') as sap_customer_channel_description,
             coalesce(actual.sap_customer_sub_channel_key,customer.sap_cust_sub_chnl_key,'NA') as sap_customer_sub_channel_key,
             coalesce(actual.sap_sub_channel_description,customer.sap_sub_chnl_desc,'NA') as sap_sub_channel_description,
             coalesce(actual.sap_parent_customer_key,customer.sap_prnt_cust_key,'NA') as sap_parent_customer_key,
             coalesce(actual.sap_parent_customer_description,customer.sap_prnt_cust_desc,'NA') as sap_parent_customer_description,
             coalesce(actual.sap_banner_key,customer.sap_bnr_key,'NA') as sap_banner_key,
             coalesce(actual.sap_banner_description,customer.sap_bnr_desc,'NA') as sap_banner_description,
             coalesce(actual.sap_banner_format_key,customer.sap_bnr_frmt_key,'NA') as sap_banner_format_key,
             coalesce(actual.sap_banner_format_description,customer.sap_bnr_frmt_desc,'NA') as sap_banner_format_description,
             trim(nvl (nullif(customer.sap_cust_nm,''),'NA')) as customer_name,
             trim(nvl (nullif(customer.sap_cust_id,''),'NA')) as customer_code,
             trim(nvl (nullif(product.sap_prod_sgmt_cd,''),'NA')) as sap_prod_sgmt_cd,
             trim(nvl (nullif(product.sap_prod_sgmt_desc,''),'NA')) as sap_prod_sgmt_desc,
             trim(nvl (nullif(product.sap_base_prod_desc,''),'NA')) as sap_base_prod_desc,
             trim(nvl (nullif(product.sap_mega_brnd_desc,''),'NA')) as sap_mega_brnd_desc,
             trim(nvl (nullif(product.sap_brnd_desc,''),'NA')) as sap_brnd_desc,
             trim(nvl (nullif(product.sap_vrnt_desc,''),'NA')) as sap_vrnt_desc,
             trim(nvl (nullif(product.sap_put_up_desc,''),'NA')) as sap_put_up_desc,
             trim(nvl (nullif(product.sap_grp_frnchse_cd,''),'NA')) as sap_grp_frnchse_cd,
             trim(nvl (nullif(product.sap_grp_frnchse_desc,''),'NA')) as sap_grp_frnchse_desc,
             trim(nvl (nullif(product.sap_frnchse_cd,''),'NA')) as sap_frnchse_cd,
             trim(nvl (nullif(product.sap_frnchse_desc,''),'NA')) as sap_frnchse_desc,
             trim(nvl (nullif(product.sap_prod_frnchse_cd,''),'NA')) as sap_prod_frnchse_cd,
             trim(nvl (nullif(product.sap_prod_frnchse_desc,''),'NA')) as sap_prod_frnchse_desc,
             trim(nvl (nullif(product.sap_prod_mjr_cd,''),'NA')) as sap_prod_mjr_cd,
             trim(nvl (nullif(product.sap_prod_mjr_desc,''),'NA')) as sap_prod_mjr_desc,
             trim(nvl (nullif(product.sap_prod_mnr_cd,''),'NA')) as sap_prod_mnr_cd,
             trim(nvl (nullif(product.sap_prod_mnr_desc,''),'NA')) as sap_prod_mnr_desc,
             trim(nvl (nullif(product.sap_prod_hier_cd,''),'NA')) as sap_prod_hier_cd,
             trim(nvl (nullif(product.sap_prod_hier_desc,''),'NA')) as sap_prod_hier_desc,
             trim(nvl (nullif(customer.sap_go_to_mdl_key,''),'NA')) as sap_go_to_mdl_key,
       upper(trim(nvl (nullif(customer.sap_go_to_mdl_desc,''),'NA'))) as sap_go_to_mdl_description,
            coalesce(actual.global_product_franchise,product.gph_prod_frnchse) as global_product_franchise,
             coalesce(actual.global_product_brand,product.gph_prod_brnd) as global_product_brand,
             coalesce(actual.global_product_sub_brand,product.gph_prod_sub_brnd) as global_product_sub_brand,
             coalesce(actual.global_product_variant,product.gph_prod_vrnt) as global_product_variant,
             coalesce(actual.global_product_segment,product.gph_prod_needstate) as global_product_segment,
             coalesce(actual.global_product_subsegment,product.gph_prod_subsgmnt) as global_product_subsegment,
             coalesce(actual.global_product_category,product.gph_prod_ctgry) as global_product_category,
             coalesce(actual.global_product_subcategory,product.gph_prod_subctgry) as global_product_subcategory,
             coalesce(actual.global_put_up_description,product.gph_prod_put_up_desc) as global_put_up_description,
             --trim(nvl (nullif(product.ean,''),'NA')) as ean,
             trim(nvl (nullif(product.sap_matl_num,''),'NA')) as sap_sku_code,
             trim(nvl (nullif(product.sap_mat_desc,''),'NA')) as sap_sku_description,
             trim(nvl (nullif(product.pka_franchise_desc,''),'NA')) as pka_franchise_desc,
             trim(nvl (nullif(product.pka_brand_desc,''),'NA')) as pka_brand_desc,
             trim(nvl (nullif(product.pka_sub_brand_desc,''),'NA')) as pka_sub_brand_desc,
             trim(nvl (nullif(product.pka_variant_desc,''),'NA')) as pka_variant_desc,
             trim(nvl (nullif(product.pka_sub_variant_desc,''),'NA')) as pka_sub_variant_desc,
             coalesce(actual.pka_product_key,product.pka_product_key) as pka_product_key,
             coalesce(actual.pka_product_key_desc,product.pka_product_key_description) as pka_product_key_description,
             actual.cm_sales as sales_value,
             actual.cm_sales_qty as sales_qty,
             actual.cm_avg_sales_qty as avg_sales_qty,
             actual.cm_sales_value_list_price as sales_value_list_price,
             actual.lm_sales as lm_sales,
             actual.lm_sales_qty as lm_sales_qty,
             actual.lm_avg_sales_qty as lm_avg_sales_qty,
             actual.lm_sales_lp as lm_sales_lp,
             actual.p3m_sales as p3m_sales,
             actual.p3m_qty as p3m_qty,
             actual.p3m_avg_qty as p3m_avg_qty,
             actual.p3m_sales_lp as p3m_sales_lp,
             actual.f3m_sales as f3m_sales,
             actual.f3m_qty as f3m_qty,
             actual.f3m_avg_qty as f3m_avg_qty,
             actual.p6m_sales as p6m_sales,
             actual.p6m_qty as p6m_qty,
             actual.p6m_avg_qty as p6m_avg_qty,
             actual.p6m_sales_lp as p6m_sales_lp,
             actual.p12m_sales as p12m_sales,
             actual.p12m_qty as p12m_qty,
             actual.p12m_avg_qty as p12m_avg_qty,
             actual.p12m_sales_lp as p12m_sales_lp,
             coalesce(actual.lm_sales_flag,'N') as lm_sales_flag,
             coalesce(actual.p3m_sales_flag,'N') as p3m_sales_flag,
             coalesce(actual.p6m_sales_flag,'N') as p6m_sales_flag,
             coalesce(actual.p12m_sales_flag,'N') as p12m_sales_flag,
             'y' as mdp_flag,
             100 as target_compliance
             from itg_cn_pc_re_msl_list target
        left join wks_cnpc_regional_sellout_actuals actual
               on target.fisc_per = actual.mnth_id
              and target.distributor_code = actual.distributor_code
              and target.soldto_code = actual.soldto_code
              and target.store_code = actual.store_code
			  and target.product_code = actual.product_code

      
        left join (select distinct edw_perfect_store_product_master.product_code,
                          edw_perfect_store_product_master.upc,
                          edw_perfect_store_product_master.product_name_cn,
                          edw_perfect_store_product_master.product_name_en,
                          edw_perfect_store_product_master.brand,
                          edw_perfect_store_product_master.layer2,
                          edw_perfect_store_product_master.layer3
                   from edw_perfect_store_product_master) epspm on nvl(ltrim (actual.mapped_sku_code,'0'),ltrim (target.mapped_sku_code,'0')) = 
                   ltrim (epspm.product_code,'0')
     ----------------customer hierarchy------------------------------
            LEFT JOIN (select * from customer_hierarchy) CUSTOMER 
            ON nvl(ltrim (actual.soldto_code,'0'),ltrim (target.soldto_code,'0')) = LTRIM (CUSTOMER.SAP_CUST_ID,'0')
            AND CUSTOMER.RANK = 1
      ----------------product hierarchy------------------------------   				   
    ----------------product hierarchy------------------------------
        LEFT JOIN (select * from product_heirarchy) PRODUCT 
        on nvl(ltrim (actual.mapped_sku_code,'0'),ltrim (target.mapped_sku_code,'0')) = ltrim (product.sap_matl_num,'0')
        AND PRODUCT.RANK = 1        
        left join (Select * from edw_product_key_attributes) prod_key1
                 on prod_key1.ctry_nm = 'China Personal Care' and nvl(ltrim (actual.mapped_sku_code,'0'),ltrim (target.mapped_sku_code,'0')) = ltrim (prod_key1.sku,'0')
				) Q,
     (select distinct cluster,
             ctry_group
      from edw_company_dim
      where ctry_group = 'China Personal Care') COM
where fisc_per <= (select max(mnth_id)
                   from wks_cnpc_regional_sellout_actuals)
)
cnpc_rpt_retail_excellence_non_mdp
as
(
select distinct q.*,
       com.cluster
from (select cast(actual.year as integer) as year,
             cast(actual.mnth_id as integer) as mnth_id,
             actual.cntry_nm as market,
             actual.data_src as data_src,
			 actual.soldto_code as soldto_code,
             actual.distributor_code as distributor_code,
             actual.distributor_name as distributor_name,
             'not defined' as sell_out_channel,
             coalesce(upper(actual.store_type),'NA') as sell_out_re,
             coalesce(actual.store_type,'NA') as store_type,
             coalesce(actual.store_code,'NA') as store_code,
             coalesce(actual.store_name,'NA') as store_name,
             coalesce(actual.region,'NA') as region,
             coalesce(actual.zone,'NA') as zone_name,
             coalesce(actual.city,'NA') as city,
			 coalesce(actual.product_code,'NA') as product_code,
			  coalesce(actual.ean,'NA') as ean,
			 nvl(actual.msl_product_desc,pd.msl_product_desc) as product_name,
             ------------------------- check 
             --actual.pka_product_key_desc as product_code,
             ---actual.sku_code as sku_code,
             --actual.pka_product_key_desc as product_name,
             'china personal care' as prod_hier_l1,
             coalesce(upper(epspm.brand::text)::character varying,'NA') as prod_hier_l4,
             coalesce(upper(epspm.layer2::text)::character varying,'NA') as prod_hier_l5,
             coalesce(epspm.layer3,'NA') as prod_hier_l6,
             coalesce(epspm.product_name_en,'NA') as prod_hier_l9,
             nvl(actual.mapped_sku_code,pd.sku_code,'NA') as mapped_sku_cd,
			 coalesce(actual.customer_segment_key,customer.cust_segmt_key,'NA') as customer_segment_key,
             coalesce(actual.customer_segment_description,customer.cust_segment_desc,'NA') as customer_segment_description,
             coalesce(actual.store_type,'NA') as retail_environment,
             coalesce(actual.sap_customer_channel_key,customer.sap_cust_chnl_key,'NA') as sap_customer_channel_key,
             coalesce(actual.sap_customer_channel_description,customer.sap_cust_chnl_desc,'NA') as sap_customer_channel_description,
             coalesce(actual.sap_customer_sub_channel_key,customer.sap_cust_sub_chnl_key,'NA') as sap_customer_sub_channel_key,
             coalesce(actual.sap_sub_channel_description,customer.sap_sub_chnl_desc,'NA') as sap_sub_channel_description,
             coalesce(actual.sap_parent_customer_key,customer.sap_prnt_cust_key,'NA') as sap_parent_customer_key,
             coalesce(actual.sap_parent_customer_description,customer.sap_prnt_cust_desc,'NA') as sap_parent_customer_description,
             coalesce(actual.sap_banner_key,customer.sap_bnr_key,'NA') as sap_banner_key,
             coalesce(actual.sap_banner_description,customer.sap_bnr_desc,'NA') as sap_banner_description,
             coalesce(actual.sap_banner_format_key,customer.sap_bnr_frmt_key,'NA') as sap_banner_format_key,
             coalesce(actual.sap_banner_format_description,customer.sap_bnr_frmt_desc,'NA') as sap_banner_format_description,
             trim(nvl (nullif(customer.sap_cust_nm,''),'NA')) as customer_name,
             trim(nvl (nullif(customer.sap_cust_id,''),'NA')) as customer_code,
             trim(nvl (nullif(product.sap_prod_sgmt_cd,''),'NA')) as sap_prod_sgmt_cd,
             trim(nvl (nullif(product.sap_prod_sgmt_desc,''),'NA')) as sap_prod_sgmt_desc,
             trim(nvl (nullif(product.sap_base_prod_desc,''),'NA')) as sap_base_prod_desc,
             trim(nvl (nullif(product.sap_mega_brnd_desc,''),'NA')) as sap_mega_brnd_desc,
             trim(nvl (nullif(product.sap_brnd_desc,''),'NA')) as sap_brnd_desc,
             trim(nvl (nullif(product.sap_vrnt_desc,''),'NA')) as sap_vrnt_desc,
             trim(nvl (nullif(product.sap_put_up_desc,''),'NA')) as sap_put_up_desc,
             trim(nvl (nullif(product.sap_grp_frnchse_cd,''),'NA')) as sap_grp_frnchse_cd,
             trim(nvl (nullif(product.sap_grp_frnchse_desc,''),'NA')) as sap_grp_frnchse_desc,
             trim(nvl (nullif(product.sap_frnchse_cd,''),'NA')) as sap_frnchse_cd,
             trim(nvl (nullif(product.sap_frnchse_desc,''),'NA')) as sap_frnchse_desc,
             trim(nvl (nullif(product.sap_prod_frnchse_cd,''),'NA')) as sap_prod_frnchse_cd,
             trim(nvl (nullif(product.sap_prod_frnchse_desc,''),'NA')) as sap_prod_frnchse_desc,
             trim(nvl (nullif(product.sap_prod_mjr_cd,''),'NA')) as sap_prod_mjr_cd,
             trim(nvl (nullif(product.sap_prod_mjr_desc,''),'NA')) as sap_prod_mjr_desc,
             trim(nvl (nullif(product.sap_prod_mnr_cd,''),'NA')) as sap_prod_mnr_cd,
             trim(nvl (nullif(product.sap_prod_mnr_desc,''),'NA')) as sap_prod_mnr_desc,
             trim(nvl (nullif(product.sap_prod_hier_cd,''),'NA')) as sap_prod_hier_cd,
             trim(nvl (nullif(product.sap_prod_hier_desc,''),'NA')) as sap_prod_hier_desc,
             trim(nvl (nullif(customer.sap_go_to_mdl_key,''),'NA')) as sap_go_to_mdl_key,
             upper(trim(nvl (nullif(customer.sap_go_to_mdl_desc,''),'NA'))) as sap_go_to_mdl_description,
             coalesce(actual.global_product_franchise,product.gph_prod_frnchse) as global_product_franchise,
             coalesce(actual.global_product_brand,product.gph_prod_brnd) as global_product_brand,
             coalesce(actual.global_product_sub_brand,product.gph_prod_sub_brnd) as global_product_sub_brand,
             coalesce(actual.global_product_variant,product.gph_prod_vrnt) as global_product_variant,
             coalesce(actual.global_product_segment,product.gph_prod_needstate) as global_product_segment,
             coalesce(actual.global_product_subsegment,product.gph_prod_subsgmnt) as global_product_subsegment,
             coalesce(actual.global_product_category,product.gph_prod_ctgry) as global_product_category,
             coalesce(actual.global_product_subcategory,product.gph_prod_subctgry) as global_product_subcategory,
             coalesce(actual.global_put_up_description,product.gph_prod_put_up_desc) as global_put_up_description,
             --trim(nvl (nullif(product.ean,''),'NA')) as ean,
             trim(nvl (nullif(product.sap_matl_num,''),'NA')) as sap_sku_code,
             trim(nvl (nullif(product.sap_mat_desc,''),'NA')) as sap_sku_description,
             trim(nvl (nullif(product.pka_franchise_desc,''),'NA')) as pka_franchise_desc,
             trim(nvl (nullif(product.pka_brand_desc,''),'NA')) as pka_brand_desc,
             trim(nvl (nullif(product.pka_sub_brand_desc,''),'NA')) as pka_sub_brand_desc,
             trim(nvl (nullif(product.pka_variant_desc,''),'NA')) as pka_variant_desc,
             trim(nvl (nullif(product.pka_sub_variant_desc,''),'NA')) as pka_sub_variant_desc,
             coalesce(actual.pka_product_key,product.pka_product_key) as pka_product_key,
             coalesce(actual.pka_product_key_desc,product.pka_product_key_description) as pka_product_key_description,
             actual.cm_sales as sales_value,
             actual.cm_sales_qty as sales_qty,
             actual.cm_avg_sales_qty as avg_sales_qty,
             actual.cm_sales_value_list_price as sales_value_list_price,
             actual.lm_sales as lm_sales,
             actual.lm_sales_qty as lm_sales_qty,
             actual.lm_avg_sales_qty as lm_avg_sales_qty,
             actual.lm_sales_lp as lm_sales_lp,
             actual.p3m_sales as p3m_sales,
             actual.p3m_qty as p3m_qty,
             actual.p3m_avg_qty as p3m_avg_qty,
             actual.p3m_sales_lp as p3m_sales_lp,
             actual.f3m_sales as f3m_sales,
             actual.f3m_qty as f3m_qty,
             actual.f3m_avg_qty as f3m_avg_qty,
             actual.p6m_sales as p6m_sales,
             actual.p6m_qty as p6m_qty,
             actual.p6m_avg_qty as p6m_avg_qty,
             actual.p6m_sales_lp as p6m_sales_lp,
             actual.p12m_sales as p12m_sales,
             actual.p12m_qty as p12m_qty,
             actual.p12m_avg_qty as p12m_avg_qty,
             actual.p12m_sales_lp as p12m_sales_lp,
             coalesce(actual.lm_sales_flag,'N') as lm_sales_flag,
             coalesce(actual.p3m_sales_flag,'N') as p3m_sales_flag,
             coalesce(actual.p6m_sales_flag,'N') as p6m_sales_flag,
             coalesce(actual.p12m_sales_flag,'N') as p12m_sales_flag,
             'n' as mdp_flag,
             100 as target_compliance
      from (select *
            from cn_wks.wks_cnpc_regional_sellout_actuals a
            where not exists (select 1
                              from cn_itg.itg_cn_pc_re_msl_list t
                              where a.mnth_id = t.fisc_per
							  and a.soldto_code = t.soldto_code
                              and   a.distributor_code = t.distributor_code
                              and   a.store_code = t.store_code
							  and   a.product_code = t.product_code
                              --and   a.store_type = t.store_type
                              --and   a.pka_product_key_desc = t.product_code
                              --and   a.zone = t.zone
                              --and   a.region = t.region
                              )
            ) actual
		left join wks_cnpc_regional_sellout_mapped_sku_cd pd on actual.ean = pd.ean
          left join (select distinct edw_perfect_store_product_master.product_code,
                          edw_perfect_store_product_master.upc,
                          edw_perfect_store_product_master.product_name_cn,
                          edw_perfect_store_product_master.product_name_en,
                          edw_perfect_store_product_master.brand,
                          edw_perfect_store_product_master.layer2,
                          edw_perfect_store_product_master.layer3
                          FROM edw_perfect_store_product_master) epspm on nvl(ltrim (actual.mapped_sku_code'0'),ltrim(pd.sku_code,'0')) = ltrim (epspm.product_code,'0')
          ----------------customer hierarchy------------------------------
        left join (select * from customer_hierarchy) customer 
            on ltrim (actual.soldto_code,'0') = ltrim (customer.sap_cust_id,'0')
      ----------------product hierarchy------------------------------   				   
      
              left join (select * from product_heirarchy) product 
        on  nvl(ltrim (actual.mapped_sku_code,'0'),ltrim (pd.sku_code,'0')) = ltrim (product.sap_matl_num,'0')
        and product.rank = 1        
        left join (select * from edw_product_key_attributes) prod_key1
                 on prod_key1.ctry_nm = 'china personal care' and on nvl(ltrim (actual.mapped_sku_code,'0'),ltrim(pd.sku_code,'0')) = ltrim (prod_key1.sku,'0')
				) q,
     (select distinct cluster,
             ctry_group
      from rg_edw.edw_company_dim
      where ctry_group = 'china personal care') com
	  where not(sales_value = 0 and sales_qty = 0 and  avg_sales_qty=0 and lm_sales=0 and lm_sales_qty= 0 and lm_avg_sales_qty=0 and
p3m_sales=0 and p3m_qty=0 and p3m_avg_qty=0 and f3m_sales=0 and f3m_qty=0 and f3m_avg_qty=0 and
p6m_sales=0 and p6m_qty=0 and p6m_avg_qty=0 and p12m_sales=0 and p12m_qty=0 and p12m_avg_qty=0 and p12m_sales_lp=0 and p6m_sales_lp=0 and 
p3m_sales_lp=0 and lm_sales_lp=0 and sales_value_list_price=0)				   
)),

wks_cnpc_rpt_retail_excellence
as
(select * from cnpc_rpt_retail_excellence_mdp
union 
cnpc_rpt_retail_excellence_non_mdp),

wks_cnpc_rpt_retail_excellence_final as
(
   select distinct fisc_yr	numeric(18,0)	as	fisc_yr,
fisc_per	numeric(18,0)	as	fisc_per,
market	varchar(30)	as	market,
data_src	varchar(14)	as	data_src,
soldto_code	varchar(255)	as	soldto_code,
distributor_code	varchar(100)	as	distributor_code,
distributor_name	varchar(356)	as	distributor_name,
sell_out_channel	varchar(11)	as	sell_out_channel,
sell_out_re	varchar(382)	as	sell_out_re,
store_type	varchar(382)	as	store_type,
store_code	varchar(100)	as	store_code,
store_name	varchar(601)	as	store_name,
region	varchar(150)	as	region,
zone_name	varchar(150)	as	zone_name,
city	varchar(150)	as	city,
product_code	varchar(200)	as	product_code,
ean	varchar(50)	as	ean,
product_name	varchar(300)	as	product_name,
prod_hier_l1	varchar(19)	as	prod_hier_l1,
prod_hier_l4	varchar(30)	as	prod_hier_l4,
prod_hier_l5	varchar(300)	as	prod_hier_l5,
prod_hier_l6	varchar(200)	as	prod_hier_l6,
prod_hier_l9	varchar(100)	as	prod_hier_l9,
mapped_sku_cd	varchar(40)	as	mapped_sku_cd,
customer_segment_key	varchar(12)	as	customer_segment_key,
customer_segment_description	varchar(50)	as	customer_segment_description,
retail_environment	varchar(382)	as	retail_environment,
sap_customer_channel_key	varchar(12)	as	sap_customer_channel_key,
sap_customer_channel_description	varchar(75)	as	sap_customer_channel_description,
sap_customer_sub_channel_key	varchar(12)	as	sap_customer_sub_channel_key,
sap_sub_channel_description	varchar(75)	as	sap_sub_channel_description,
sap_parent_customer_key	varchar(12)	as	sap_parent_customer_key,
sap_parent_customer_description	varchar(75)	as	sap_parent_customer_description,
sap_banner_key	varchar(12)	as	sap_banner_key,
sap_banner_description	varchar(75)	as	sap_banner_description,
sap_banner_format_key	varchar(12)	as	sap_banner_format_key,
sap_banner_format_description	varchar(75)	as	sap_banner_format_description,
customer_name	varchar(100)	as	customer_name,
customer_code	varchar(10)	as	customer_code,
sap_prod_sgmt_cd	varchar(18)	as	sap_prod_sgmt_cd,
sap_prod_sgmt_desc	varchar(100)	as	sap_prod_sgmt_desc,
sap_base_prod_desc	varchar(100)	as	sap_base_prod_desc,
sap_mega_brnd_desc	varchar(100)	as	sap_mega_brnd_desc,
sap_brnd_desc	varchar(100)	as	sap_brnd_desc,
sap_vrnt_desc	varchar(100)	as	sap_vrnt_desc,
sap_put_up_desc	varchar(100)	as	sap_put_up_desc,
sap_grp_frnchse_cd	varchar(18)	as	sap_grp_frnchse_cd,
sap_grp_frnchse_desc	varchar(100)	as	sap_grp_frnchse_desc,
sap_frnchse_cd	varchar(18)	as	sap_frnchse_cd,
sap_frnchse_desc	varchar(100)	as	sap_frnchse_desc,
sap_prod_frnchse_cd	varchar(18)	as	sap_prod_frnchse_cd,
sap_prod_frnchse_desc	varchar(100)	as	sap_prod_frnchse_desc,
sap_prod_mjr_cd	varchar(18)	as	sap_prod_mjr_cd,
sap_prod_mjr_desc	varchar(100)	as	sap_prod_mjr_desc,
sap_prod_mnr_cd	varchar(18)	as	sap_prod_mnr_cd,
sap_prod_mnr_desc	varchar(100)	as	sap_prod_mnr_desc,
sap_prod_hier_cd	varchar(18)	as	sap_prod_hier_cd,
sap_prod_hier_desc	varchar(100)	as	sap_prod_hier_desc,
sap_go_to_mdl_key	varchar(12)	as	sap_go_to_mdl_key,
sap_go_to_mdl_description	varchar(75)	as	sap_go_to_mdl_description,
global_product_franchise	varchar(30)	as	global_product_franchise,
global_product_brand	varchar(30)	as	global_product_brand,
global_product_sub_brand	varchar(100)	as	global_product_sub_brand,
global_product_variant	varchar(100)	as	global_product_variant,
global_product_segment	varchar(50)	as	global_product_segment,
global_product_subsegment	varchar(100)	as	global_product_subsegment,
global_product_category	varchar(50)	as	global_product_category,
global_product_subcategory	varchar(50)	as	global_product_subcategory,
global_put_up_description	varchar(100)	as	global_put_up_description,
sap_sku_code	varchar(40)	as	sap_sku_code,
sap_sku_description	varchar(100)	as	sap_sku_description,
pka_franchise_desc	varchar(30)	as	pka_franchise_desc,
pka_brand_desc	varchar(30)	as	pka_brand_desc,
pka_sub_brand_desc	varchar(30)	as	pka_sub_brand_desc,
pka_variant_desc	varchar(30)	as	pka_variant_desc,
pka_sub_variant_desc	varchar(30)	as	pka_sub_variant_desc,
pka_product_key	varchar(68)	as	pka_product_key,
pka_product_key_description	varchar(382)	as	pka_product_key_description,
sales_value	numeric(38,6)	as	sales_value,
sales_qty	numeric(38,6)	as	sales_qty,
avg_sales_qty	numeric(38,6)	as	avg_sales_qty,
sales_value_list_price	numeric(38,12)	as	sales_value_list_price,
lm_sales	numeric(38,6)	as	lm_sales,
lm_sales_qty	numeric(38,6)	as	lm_sales_qty,
lm_avg_sales_qty	numeric(38,6)	as	lm_avg_sales_qty,
lm_sales_lp	numeric(38,12)	as	lm_sales_lp,
p3m_sales	numeric(38,6)	as	p3m_sales,
p3m_qty	numeric(38,6)	as	p3m_qty,
p3m_avg_qty	numeric(38,6)	as	p3m_avg_qty,
p3m_sales_lp	numeric(38,12)	as	p3m_sales_lp,
f3m_sales	numeric(38,6)	as	f3m_sales,
f3m_qty	numeric(38,6)	as	f3m_qty,
f3m_avg_qty	numeric(38,6)	as	f3m_avg_qty,
p6m_sales	numeric(38,6)	as	p6m_sales,
p6m_qty	numeric(38,6)	as	p6m_qty,
p6m_avg_qty	numeric(38,6)	as	p6m_avg_qty,
p6m_sales_lp	numeric(38,12)	as	p6m_sales_lp,
p12m_sales	numeric(38,6)	as	p12m_sales,
p12m_qty	numeric(38,6)	as	p12m_qty,
p12m_avg_qty	numeric(38,6)	as	p12m_avg_qty,
p12m_sales_lp	numeric(38,12)	as	p12m_sales_lp,
lm_sales_flag	varchar(1)	as	lm_sales_flag,
p3m_sales_flag	varchar(1)	as	p3m_sales_flag,
p6m_sales_flag	varchar(1)	as	p6m_sales_flag,
p12m_sales_flag	varchar(1)	as	p12m_sales_flag,
mdp_flag	varchar(1)	as	mdp_flag,
target_compliance	numeric(18,0)	as	target_compliance,
"cluster"	varchar(100)	as	"cluster"
from wks_cnpc_rpt_retail_excellence
)



