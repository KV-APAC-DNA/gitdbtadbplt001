{{
    config(
        sql_header='use warehouse DEV_DNA_CORE_app2_wh;'
    )
}}

with v_edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
itg_mds_in_ecom_nts_adjustment as(
    select * from {{ ref('inditg_integration__itg_mds_in_ecom_nts_adjustment') }}
),
edw_copa_trans_fact as(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_pharmacy_ecommerce_analysis as(
	select * from {{ ref('SNAPPCFEDW_INTEGRATION__EDW_PHARMACY_ECOMMERCE_ANALYSIS') }}
),
edw_profit_center_franchise_mapping as(
    select * from {{ ref('aspedw_integration__edw_profit_center_franchise_mapping') }}
),
edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
wks_filter_params as(
    select * from {{ ref('aspwks_integration__wks_filter_params') }}
),
itg_mds_ap_ecom_oneview_config as(
    select * from {{ ref('aspitg_integration__itg_mds_ap_ecom_oneview_config') }}
),
wks_india_ecom as(
    select * from {{ ref('INDWKS_INTEGRATION__WKS_INDIA_ECOM') }}
),
edw_material_plant_dim as(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
dm_integration_dly as(
    select * from {{ ref('jpnedw_integration__dm_integration_dly') }}
),
edi_chn_m as(
    select * from {{ ref('jpnedw_integration__edi_chn_m') }}
),
edi_item_m as(
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
),
mt_cld as(
    select * from {{ ref('jpnedw_integration__mt_cld') }}
),
mt_account_key as(
    select * from {{ ref('jpnedw_integration__mt_account_key') }}
),
V_INTRM_DISC_REBATE_YTD as(
select * from {{ ref('ASPEDW_INTEGRATION__V_INTRM_DISC_REBATE_YTD') }}
),
MT_TP_STATUS_MAPPING as(
    select * from {{ ref('JPNEDW_INTEGRATION__MT_TP_STATUS_MAPPING') }}
),
edw_ecommerce_nts_regional as(
    select * from {{ ref('ASPEDW_INTEGRATION__EDW_ECOMMERCE_NTS_REGIONAL') }}
),
itg_query_parameters as(
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
edw_ap_ecomm_nts_manual_adjustment as(
    select * from {{ ref('ASPEDW_INTEGRATION__EDW_AP_ECOMM_NTS_MANUAL_ADJUSTMENT') }}
),
edw_ecomm_plan as(
    select * from {{ ref('aspedw_integration__edw_ecomm_plan') }}
),
v_rpt_pos_offtake_daily as(
    select * from {{ ref('NTAEDW_INTEGRATION__V_RPT_POS_OFFTAKE_DAILY') }}
),
edw_ecommerce_offtake as(
    select * from {{ ref('indedw_integration__edw_ecommerce_offtake') }}
),
edw_product_key_attributes as(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
edw_customer_base_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_ims_fact as(
    select * from {{ ref('NTAEDW_INTEGRATION__EDW_IMS_FACT') }}
),
edw_ph_ecommerce_offtake as(
    select * from {{ ref('SNAPOSEEDW_INTEGRATION__EDW_PH_ECOMMERCE_OFFTAKE') }}
),
edw_ecommerce_offtake_nta as(
select * from {{ ref('SNAPNTAEDW_INTEGRATION__EDW_ECOMMERCE_OFFTAKE') }}
),
edw_sg_sellin_analysis as(
	select * from {{ ref('SNAPOSEEDW_INTEGRATION__EDW_SG_SELLIN_ANALYSIS') }}
),
edw_acct_ciw_hier as(
	select * from {{ ref('aspedw_integration__edw_acct_ciw_hier') }}
),
edw_account_ciw_dim as(
	select * from {{ ref('aspedw_integration__edw_account_ciw_dim') }}
),
edw_gch_producthierarchy as (
	select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
itg_mds_ap_sales_ops_map as(
	select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),
edw_market_share_qsd as(
	select * from {{ ref('ASPEDW_INTEGRATION__EDW_MARKET_SHARE_QSD') }}
),
edw_gch_customerhierarchy as (
	select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
cus_sales_extn as(
    select cus_sales.sls_org  ,
            cus_sales.dstr_chnl,
            cus_sales.div      ,
            cus_sales.cust_num ,
            cus_sales."parent customer",
            cus_sales.banner,
            cus_sales."banner format",
            cus_sales.channel,
            cus_sales."go to model",
            cus_sales."sub channel",
            cus_sales.retail_env
    from v_edw_customer_sales_dim cus_sales  
),
insert1 as(
    SELECT 'Act'::CHARACTER VARYING AS data_type,
        'COPA' as Datasource,
        'SKU' as Data_level,
            copa.acct_hier_shrt_desc as KPI,	   
        'Monthly' as Period_type,
        copa.fisc_yr::CHARACTER VARYING AS fisc_year,
            null as cal_year,
        LTRIM("substring" (copa.fisc_yr_per::CHARACTER VARYING::TEXT,5,3),'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
            null as cal_month,
        TO_DATE(("substring" (copa.fisc_yr_per::CHARACTER VARYING::TEXT,6,8) || '01'::CHARACTER VARYING::TEXT) || "substring" (copa.fisc_yr_per::CHARACTER VARYING::TEXT,1,4),'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
            null as cal_day,
        copa.fisc_yr_per,
        filter_params."cluster",	   
        filter_params.ctry_group AS ctry_nm,
        filter_params.ctry_group AS sub_country,             
        cus_sales_extn.channel,
        cus_sales_extn."sub channel",
        cus_sales_extn.retail_env,       
        cus_sales_extn."go to model",
        LTRIM(copa.prft_ctr::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS profit_center,   
        filter_params.co_cd AS company_code,    
        copa.cust_num AS customer_code,       
        cus_sales_extn."parent customer",
        cus_sales_extn.banner,
        cus_sales_extn."banner format",
        null as platform_name,
        null as retailer_name,
        null as retailer_name_english,
        'Johnson & Johnson' as manufacturer_name,
        'Y' as jj_manufacturer_flag,
        prod_map.franchise_l1 as prod_hier_l1,	   
        prod_map.need_state as prod_hier_l2,	
        'na' as prod_hier_l3,
        'na' as prod_hier_l4,
        mat.mega_brnd_desc AS prod_hier_l5,
        mat.brnd_desc AS prod_hier_l6, 
        mat.varnt_desc AS prod_hier_l7,
        mat.put_up_desc AS prod_hier_l8,	   
        mat.matl_desc AS prod_hier_l9,
        mat.prodh5 AS product_minor_code,
        prod_map.prod_minor AS product_minor_name,
        LTRIM(copa.matl_num::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS material_number,	   
        null as ean,
        null as reailer_sku_code,
        mat.pka_product_key,
        mat.pka_product_key_description,  
        0 AS target_ori,
        0 AS value,	  
        SUM(copa.amt_obj_crncy*exch_rate.ex_rt) as usd_value,
        SUM(copa.amt_obj_crncy) as lcy_value,
        0 AS salesweight,
        exch_rate.from_crncy,
        exch_rate.to_crncy,
        'na' AS acct_nm,
        'na' AS acct_num,
        'na' AS ciw_desc,
        'na' AS ciw_bucket,
        'na' AS csw_desc,
        'na' AS "Additional_Information",
        NULL as ppm_role  
            
    FROM edw_copa_trans_fact copa
    LEFT JOIN edw_material_dim mat ON copa.matl_num::TEXT = mat.matl_num::TEXT
    LEFT JOIN edw_profit_center_franchise_mapping prod_map ON TRIM (copa.prft_ctr::TEXT,'0'::CHARACTER VARYING::TEXT) = TRIM (prod_map.profit_center::TEXT,'0'::CHARACTER VARYING::TEXT)
    LEFT JOIN cus_sales_extn 
            ON copa.sls_org  =   cus_sales_extn.sls_org 
            AND copa.dstr_chnl  = cus_sales_extn.dstr_chnl 
            AND copa.div       =  cus_sales_extn.div 
            AND copa.cust_num  =  cus_sales_extn.cust_num 
    JOIN  (select distinct ctry_key, ctry_group, "cluster", co_cd, gts, nts, min_year, cust_filter from wks_filter_params) filter_params --wks_filter_params 
        ON copa.co_cd  = filter_params.co_cd  
        AND (copa.acct_hier_shrt_desc = filter_params.gts OR copa.acct_hier_shrt_desc = filter_params.nts )
        AND copa.fisc_yr >=  filter_params.min_year 
        AND copa.cust_num = nvl(filter_params.cust_filter, copa.cust_num)
        --AND filter_params.ctry_key = 'JP'     
    LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
            ON copa.obj_crncy_co_obj  = exch_rate.from_crncy 
            AND exch_rate.to_crncy  = 'USD' 
            AND copa.fisc_yr_per = exch_rate.fisc_per    
    JOIN (select distinct ctry_key, retail_env from wks_filter_params) fp
    ON copa.ctry_key =  fp.ctry_key   
        AND cus_sales_extn.retail_env = nvl(fp.retail_env, cus_sales_extn.retail_env)
                            
    GROUP BY  copa.acct_hier_shrt_desc ,	   
        copa.fisc_yr,
        copa.fisc_yr_per,
        filter_params."cluster",	   
        filter_params.ctry_group ,
        filter_params.ctry_group ,             
        cus_sales_extn.channel,
        cus_sales_extn."sub channel",
        cus_sales_extn.retail_env,       
        cus_sales_extn."go to model",
        LTRIM(copa.prft_ctr::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING ,   
        filter_params.co_cd ,    
        copa.cust_num ,       
        cus_sales_extn."parent customer",
        cus_sales_extn.banner,
        cus_sales_extn."banner format",
        prod_map.franchise_l1 ,	   
        prod_map.need_state ,	
        mat.mega_brnd_desc  ,
        mat.brnd_desc  , 
        mat.varnt_desc  ,
        mat.put_up_desc  ,	   
        mat.matl_desc  ,
        mat.prodh5  ,
        prod_map.prod_minor  ,
        LTRIM(copa.matl_num::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING ,	   
        mat.pka_product_key,
        mat.pka_product_key_description,  
        exch_rate.from_crncy,
        exch_rate.to_crncy 
),
CN_Customer AS (
		SELECT DISTINCT copa.cust_num,
			copa.co_cd,
			company.ctry_key,
			company.ctry_group,
			company."cluster"
		FROM edw_copa_trans_fact copa
		JOIN edw_company_dim company ON copa.co_cd::TEXT = company.co_cd::TEXT
		WHERE company.ctry_key::TEXT = (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'ctry_key'
					AND dataset = 'China SAP (Domestic)'
				)
			AND copa.acct_hier_shrt_desc::TEXT IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'acct_hier_shrt_desc'
					AND dataset = 'China SAP (Domestic)'
				)
			AND copa.sls_ofc IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'sls_ofc'
					AND dataset = 'China SAP (Domestic)'
				)
			AND copa.fisc_yr >= (
				SELECT fisc_yr
				FROM edw_calendar_dim
				WHERE cal_day = to_date(current_timestamp())
				) - (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'fisc_yr'
					AND dataset = 'China SAP (Domestic)'
				)
		
		UNION ALL
		
		SELECT DISTINCT copa.cust_num,
			copa.co_cd,
			company.ctry_key,
			company.ctry_group,
			company."cluster"
		FROM edw_copa_trans_fact copa
		JOIN edw_company_dim company ON copa.co_cd::TEXT = company.co_cd::TEXT
		WHERE copa.cust_num IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'cust_num'
					AND dataset = 'China SAP (FTZ)'
				)
			AND company.co_cd IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'co_cd'
					AND dataset = 'China SAP (FTZ)'
				)
			AND copa.acct_hier_shrt_desc::TEXT IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'acct_hier_shrt_desc'
					AND dataset = 'China SAP (Domestic)'
				)
			AND copa.fisc_yr >= (
				SELECT fisc_yr
				FROM edw_calendar_dim
				WHERE cal_day = to_date(current_timestamp())
				) - (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE column_name = 'fisc_yr'
					AND dataset = 'China SAP (Domestic)'
				)
),
insert2 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'COPA' AS Datasource,
		'SKU' AS Data_level,
		copa.acct_hier_shrt_desc AS KPI,
		'Monthly' AS Period_type,
		copa.fisc_yr::CHARACTER VARYING AS fisc_year,
		NULL AS cal_year,
		LTRIM("substring" (
				copa.fisc_yr_per::CHARACTER VARYING::TEXT,
				5,
				3
				), '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
		NULL AS cal_month,
		TO_DATE((
				"substring" (
					copa.fisc_yr_per::CHARACTER VARYING::TEXT,
					6,
					6
					) || '01'::CHARACTER VARYING::TEXT
				) || "substring" (
				copa.fisc_yr_per::CHARACTER VARYING::TEXT,
				1,
				2
				), 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		NULL AS cal_day,
		copa.fisc_yr_per,
		'China' AS "cluster",
		'China Personal Care' AS ctry_nm,
		'China Personal Care' AS sub_country,
		cus_sales_extn.channel,
		cus_sales_extn."sub channel",
		cus_sales_extn.retail_env,
		cus_sales_extn."go to model",
		LTRIM(copa.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS profit_center,
		copa.co_cd AS company_code,
		copa.cust_num AS customer_code,
		cus_sales_extn."parent customer",
		cus_sales_extn.banner,
		cus_sales_extn."banner format",
		NULL AS platform_name,
		NULL AS retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		prod_map.franchise_l1 AS prod_hier_l1,
		prod_map.need_state AS prod_hier_l2,
		'na' AS prod_hier_l3,
		'na' AS prod_hier_l4,
		mat.mega_brnd_desc AS prod_hier_l5,
		mat.brnd_desc AS prod_hier_l6,
		mat.varnt_desc AS prod_hier_l7,
		mat.put_up_desc AS prod_hier_l8,
		mat.matl_desc AS prod_hier_l9,
		mat.prodh5 AS product_minor_code,
		prod_map.prod_minor AS product_minor_name,
		LTRIM(copa.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS material_number,
		NULL AS ean,
		NULL AS reailer_sku_code,
		mat.pka_product_key,
		mat.pka_product_key_description,
		0 AS target_ori,
		0 AS value,
		SUM(copa.amt_obj_crncy * exch_rate.ex_rt) AS usd_value,
		SUM(copa.amt_obj_crncy) AS lcy_value,
		0 AS salesweight,
		exch_rate.from_crncy,
		exch_rate.to_crncy,
		'na' AS acct_nm,
		'na' AS acct_num,
		'na' AS ciw_desc,
		'na' AS ciw_bucket,
		'na' AS csw_desc,
		'na' AS "Additional_Information",
		NULL AS ppm_role
	FROM edw_copa_trans_fact copa
	LEFT JOIN edw_material_dim mat ON copa.matl_num::TEXT = mat.matl_num::TEXT
	LEFT JOIN edw_profit_center_franchise_mapping prod_map ON LTRIM(copa.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
	LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON copa.sls_org::TEXT = cus_sales_extn.sls_org::TEXT
		AND copa.dstr_chnl::TEXT = cus_sales_extn.dstr_chnl::TEXT
		AND copa.div::TEXT = cus_sales_extn.div::TEXT
		AND copa.cust_num::TEXT = cus_sales_extn.cust_num::TEXT
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON copa.obj_crncy_co_obj::TEXT = exch_rate.from_crncy::TEXT
		AND exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT
		AND copa.fisc_yr_per = exch_rate.fisc_per
	JOIN CN_Customer c ON copa.cust_num = c.cust_num
		AND copa.co_cd = c.co_cd
		AND copa.acct_hier_shrt_desc::TEXT IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'acct_hier_shrt_desc'
				AND dataset = 'China SAP (Domestic)'
			)
		AND copa.fisc_yr >= (
			SELECT fisc_yr
			FROM edw_calendar_dim
			WHERE cal_day = to_date(current_timestamp())
			) - (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'fisc_yr'
				AND dataset = 'China SAP (Domestic)'
			)
	GROUP BY copa.acct_hier_shrt_desc,
		copa.fisc_yr,
		copa.fisc_yr_per,
		cus_sales_extn.channel,
		cus_sales_extn."sub channel",
		cus_sales_extn.retail_env,
		cus_sales_extn."go to model",
		LTRIM(copa.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING,
		copa.co_cd,
		copa.cust_num,
		cus_sales_extn."parent customer",
		cus_sales_extn.banner,
		cus_sales_extn."banner format",
		prod_map.franchise_l1,
		prod_map.need_state,
		mat.mega_brnd_desc,
		mat.brnd_desc,
		mat.varnt_desc,
		mat.put_up_desc,
		mat.matl_desc,
		mat.prodh5,
		prod_map.prod_minor,
		LTRIM(copa.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING,
		mat.pka_product_key,
		mat.pka_product_key_description,
		exch_rate.from_crncy,
		exch_rate.to_crncy
),
mat as(SELECT edw_material_dim.matl_num,
                    edw_material_dim.prodh1_txtmd,
                    edw_material_dim.prodh2_txtmd,
                    edw_material_dim.prodh3_txtmd,
                    edw_material_dim.prodh4_txtmd,
                    edw_material_dim.prodh5_txtmd,
                    edw_material_dim.prodh6_txtmd,
                    edw_material_dim.mega_brnd_desc,
                    edw_material_dim.brnd_desc,
                    edw_material_dim.base_prod_desc,
                    edw_material_dim.varnt_desc,
                    edw_material_dim.put_up_desc,
                    edw_material_dim.matl_desc,
                    edw_material_dim.prodh5,
                    edw_material_dim.pka_product_key,
                    edw_material_dim.pka_product_key_description
             FROM edw_material_dim
             GROUP BY edw_material_dim.matl_num,
                      edw_material_dim.prodh1_txtmd,
                      edw_material_dim.prodh2_txtmd,
                      edw_material_dim.prodh3_txtmd,
                      edw_material_dim.prodh4_txtmd,
                      edw_material_dim.prodh5_txtmd,
                      edw_material_dim.prodh6_txtmd,
                      edw_material_dim.mega_brnd_desc,
                      edw_material_dim.brnd_desc,
                      edw_material_dim.base_prod_desc,
                      edw_material_dim.varnt_desc,
                      edw_material_dim.put_up_desc,
                      edw_material_dim.matl_desc,
                      edw_material_dim.prodh5,
                      edw_material_dim.pka_product_key,
                      edw_material_dim.pka_product_key_description
),
insert3 as(
SELECT 'Act'::CHARACTER VARYING AS data_type,
       'ka_sales_fact' as Datasource,
       'SKU' as Data_level,
       'GTS' as KPI,	   
       'Monthly' as Period_type,
       in_sales.fisc_yr::CHARACTER VARYING AS fisc_year,
	   null as cal_year,
       in_sales.mth_yyyymm::CHARACTER VARYING AS fisc_month,
	   null as cal_month,
       TO_DATE((LPAD(in_sales.mth_yyyymm,2,'0') || '01') || in_sales.fisc_yr,'MMDDYYYY') AS fisc_day,
	   null as cal_day,
       (in_sales.fisc_yr || LPAD(in_sales.mth_yyyymm,2,'0'))::INTEGER AS fisc_yr_per,
       'India'::CHARACTER VARYING AS "cluster",
       'India'::CHARACTER VARYING AS ctry_nm,
       'India' AS sub_country,	   
       'na'::CHARACTER VARYING AS channel,
       'na'::CHARACTER VARYING AS "sub channel",
       'na'::CHARACTER VARYING AS retail_env,
       'Indirect Accounts'::CHARACTER VARYING AS "go to model",
       LTRIM(mat_plant.prft_ctr::TEXT,'0')::CHARACTER VARYING AS profit_center,   
       'na' AS company_code,
       in_sales.customer_code,
       in_sales.parent_name AS "parent customer",
       'na'::CHARACTER VARYING AS banner,
       'na'::CHARACTER VARYING AS "banner format",
	   null as platform_name,
	   null as retailer_name,
	   null as retailer_name_english,
	   'Johnson & Johnson' as manufacturer_name,
	   'Y' as jj_manufacturer_flag,
       prod_map.franchise_l1 as prod_hier_l1,
       prod_map.need_state as prod_hier_l2,  
	   'na' as prod_hier_l3,
	   'na' as prod_hier_l4,
       mat.mega_brnd_desc AS prod_hier_l5,
       mat.brnd_desc AS prod_hier_l6,	   
       mat.varnt_desc AS  prod_hier_l7,	
       mat.put_up_desc AS prod_hier_l8,	   
       mat.matl_desc AS prod_hier_l9,   	   
       mat.prodh5 AS product_minor_code,
       prod_map.prod_minor AS product_minor_name, 
       LTRIM(in_sales.product_code::TEXT,'0')::CHARACTER VARYING AS product_code,
	   null as ean,
	   null as reailer_sku_code,  
       mat.pka_product_key,
       mat.pka_product_key_description,
       0 AS target_ori,
       0 AS value,	   
       SUM(in_sales.totalsalesnrconfirmed*exch_rate.ex_rt) AS usd_value,
       SUM(in_sales.totalsalesnrconfirmed) AS lcy_value,
	   0 AS salesweight,
       exch_rate.from_crncy,
       exch_rate.to_crncy,	   
       'na' AS acct_nm,
       'na' AS acct_num,
       'na' AS ciw_desc,
       'na' AS ciw_bucket,
       'na' AS csw_desc,
       'na' AS "Additional_Information"	,
       NULL as ppm_role    
    
FROM  wks_india_ecom in_sales
  LEFT JOIN  mat ON LTRIM (in_sales.product_code::TEXT,'0') = LTRIM (mat.matl_num::TEXT,'0')
  LEFT JOIN (SELECT edw_material_plant_dim.matl_num,
                    edw_material_plant_dim.prft_ctr
             FROM edw_material_plant_dim
             GROUP BY edw_material_plant_dim.matl_num,
                      edw_material_plant_dim.prft_ctr) mat_plant ON LTRIM (in_sales.product_code::TEXT,'0') = LTRIM (mat_plant.matl_num::TEXT,'0')
  LEFT JOIN edw_profit_center_franchise_mapping prod_map ON LTRIM (mat_plant.prft_ctr::TEXT,'0') = LTRIM (prod_map.profit_center::TEXT,'0')
  LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
         ON exch_rate.from_crncy  = (select filter_value  
                               from itg_mds_ap_ecom_oneview_config        
                               where column_name = 'from_crncy'
                                and dataset = 'India (DMS)' )
        AND exch_rate.to_crncy::TEXT = (select filter_value  
                               from itg_mds_ap_ecom_oneview_config        
                               where column_name = 'to_crncy'
                                and dataset = 'India (DMS)' )
        AND ( ( (in_sales.fisc_yr || LPAD (in_sales.mth_yyyymm,3,'0'))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per
WHERE in_sales.parent_code IN ( select filter_value  
                               from itg_mds_ap_ecom_oneview_config        
                               where column_name = 'parent_code'
                                and dataset = 'India (DMS)' )
AND   in_sales.fisc_yr >=  ( select (select fisc_yr 
                                       from edw_calendar_dim 
                                      where cal_day = to_date(current_timestamp())) - filter_value as filter_value 
                               from itg_mds_ap_ecom_oneview_config        
                               where column_name = 'fisc_yr'
                                and dataset = 'India (DMS)' )
GROUP BY 
       in_sales.fisc_yr,
       in_sales.mth_yyyymm,
       LTRIM(mat_plant.prft_ctr::TEXT,'0')::CHARACTER VARYING ,   
       in_sales.customer_code,
       in_sales.parent_name ,
       prod_map.franchise_l1 ,
       prod_map.need_state   , 
       mat.mega_brnd_desc  ,
       mat.brnd_desc       ,
       mat.varnt_desc      ,
       mat.put_up_desc     ,
       mat.matl_desc       ,  
       mat.prodh5          ,
       prod_map.prod_minor , 
       LTRIM(in_sales.product_code::TEXT,'0')::CHARACTER VARYING ,
       mat.pka_product_key,
       mat.pka_product_key_description,
       exch_rate.from_crncy,
       exch_rate.to_crncy 
),
insert4 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'ka_sales_fact' as Datasource,
		'SKU' as Data_level,
		'NTS' as KPI,
		'Monthly' as Period_type,
		in_sales.fisc_yr::CHARACTER VARYING AS fisc_year,
		null as cal_year,
		in_sales.mth_yyyymm::CHARACTER VARYING AS fisc_month,
		null AS cal_month,
		TO_DATE((LPAD(in_sales.mth_yyyymm,2,'0') || '01') || in_sales.fisc_yr,'MMDDYYYY') AS fisc_day,
		null as cal_day,
		(in_sales.fisc_yr || LPAD(in_sales.mth_yyyymm,2,'0'))::INTEGER AS fisc_yr_per,
		'India'::CHARACTER VARYING AS "cluster",
		'India'::CHARACTER VARYING AS ctry_nm,
		'India' AS sub_country,	   
		'na'::CHARACTER VARYING AS channel,
		'na'::CHARACTER VARYING AS "sub channel",
		'na'::CHARACTER VARYING AS retail_env,
		'Indirect Accounts'::CHARACTER VARYING AS "go to model",	   
		LTRIM(mat_plant.prft_ctr::TEXT,'0')::CHARACTER VARYING AS profit_center,
		'na' AS company_code,
		in_sales.customer_code,
		in_sales.parent_name AS "parent customer",
		'na'::CHARACTER VARYING AS banner,
		'na'::CHARACTER VARYING AS "banner format",
		null as platform_name,
		null as retailer_name,
		null as retailer_name_english,
		'Johnson & Johnson' as manufacturer_name,
		'Y' as jj_manufacturer_flag,
		prod_map.franchise_l1  as prod_hier_l1, 
		prod_map.need_state    as prod_hier_l2,
		'na' as prod_hier_l3,
		'na' as prod_hier_l4,
		mat.mega_brnd_desc     AS prod_hier_l5,
		mat.brnd_desc          AS prod_hier_l6,	   
		mat.varnt_desc         AS prod_hier_l7,
		mat.put_up_desc        AS prod_hier_l8,   
		mat.matl_desc 		  AS prod_hier_l9, 
		mat.prodh5 AS product_minor_code,
		prod_map.prod_minor AS product_minor_name, 
		LTRIM(in_sales.product_code::TEXT,'0')::CHARACTER VARYING AS product_code,
		null as ean,
		null as reailer_sku_code,
		mat.pka_product_key,
		mat.pka_product_key_description,
		0 AS target_ori,	   
		0 AS value,
		SUM(cast(in_sales.totalsalesnrconfirmed* cast(adj.value as numeric(31,4) ) as numeric(31,4) )  *exch_rate.ex_rt ) usd_value,
		SUM(cast(in_sales.totalsalesnrconfirmed* cast(adj.value as numeric(31,4) ) as numeric(31,4) )  ) lcy_value,       
		0 as salesweight,
		exch_rate.from_crncy,
		exch_rate.to_crncy,
		'na' AS acct_nm,
		'na' AS acct_num,
		'na' AS ciw_desc,
		'na' AS ciw_bucket,
		'na' AS csw_desc,
		'na' AS "Additional_Information"	,
		NULL as ppm_role     
		

	FROM  wks_india_ecom in_sales
	JOIN  itg_mds_in_ecom_nts_adjustment adj
		ON  (in_sales.fisc_yr ||lpad(in_sales.mth_yyyymm,2,'0') >= valid_from 
			AND in_sales.fisc_yr ||lpad(in_sales.mth_yyyymm,2,'0') <= valid_to )
	LEFT JOIN mat ON LTRIM (in_sales.product_code::TEXT,'0') = LTRIM (mat.matl_num::TEXT,'0')
	LEFT JOIN (SELECT edw_material_plant_dim.matl_num,
						edw_material_plant_dim.prft_ctr
				FROM edw_material_plant_dim
				GROUP BY edw_material_plant_dim.matl_num,
						edw_material_plant_dim.prft_ctr) mat_plant ON LTRIM (in_sales.product_code::TEXT,'0') = LTRIM (mat_plant.matl_num::TEXT,'0')
	LEFT JOIN edw_profit_center_franchise_mapping prod_map ON LTRIM (mat_plant.prft_ctr::TEXT,'0') = LTRIM (prod_map.profit_center::TEXT,'0')
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
			ON exch_rate.from_crncy  = (select filter_value  
								from itg_mds_ap_ecom_oneview_config        
								where column_name = 'from_crncy'
									and dataset = 'India (DMS)' )
			AND exch_rate.to_crncy::TEXT = (select filter_value  
								from itg_mds_ap_ecom_oneview_config        
								where column_name = 'to_crncy'
									and dataset = 'India (DMS)' )
			AND ( ( (in_sales.fisc_yr || LPAD (in_sales.mth_yyyymm,3,'0'))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per
	WHERE in_sales.parent_code IN ( select filter_value  
								from itg_mds_ap_ecom_oneview_config        
								where column_name = 'parent_code'
									and dataset = 'India (DMS)' )
	AND   in_sales.fisc_yr >=  ( select (select fisc_yr 
										from edw_calendar_dim 
										where cal_day = to_date(current_timestamp())) - filter_value as filter_value 
								from itg_mds_ap_ecom_oneview_config        
								where column_name = 'fisc_yr'
									and dataset = 'India (DMS)' )
	GROUP BY 
		in_sales.fisc_yr ,
		in_sales.mth_yyyymm ,
		LTRIM(mat_plant.prft_ctr::TEXT,'0')::CHARACTER VARYING ,
		in_sales.customer_code,
		in_sales.parent_name   ,
		prod_map.franchise_l1  ,
		prod_map.need_state    ,
		mat.mega_brnd_desc     ,
		mat.brnd_desc          ,
		mat.varnt_desc         ,
		mat.put_up_desc        ,
		mat.matl_desc 		  ,
		mat.prodh5 			  ,
		prod_map.prod_minor    ,
		LTRIM(in_sales.product_code::TEXT,'0')::CHARACTER VARYING ,
		mat.pka_product_key,
		mat.pka_product_key_description,
		exch_rate.from_crncy,
		exch_rate.to_crncy

),
union1 as(
		SELECT a.jcp_plan_type AS data_type,
		'JP_DMS' AS datasource,
		'SKU' AS data_level,
		'GTS' AS kpi,
		'Continuous' AS period_type,
		e.year_445 AS fisc_year,
		e.year AS cal_year,
		e.month_445::INTEGER AS fisc_month,
		e.month AS cal_month,
		to_date(e.ymd_dt) AS fisc_day,
		to_date(e.ymd_dt) AS cal_day,
		(e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) AS fisc_yr_per,
		'Japan' AS "cluster",
		'Japan' AS market,
		'Japan' AS sub_market,
		'E-Commerce' AS channel,
		'E-Commerce' AS sub_channel,
		'E-Commerce' AS retail_environment,
		'Indirect Accounts' AS go_to_model,
		mat_plant.prft_ctr AS profit_center,
		'Not Available' AS company_code,
		a.jcp_chn_cd AS sap_customer_code,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END AS sap_customer_name,
		NULL AS banner,
		NULL AS banner_format,
		NULL AS platform_name,
		b.lgl_nm AS retailer_name,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'Amazon'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		prod_map.franchise_l1 AS prod_hier_l1,
		prod_map.need_state AS prod_hier_l2,
		NULL AS prod_hier_l3,
		NULL AS prod_hier_l4,
		g.mega_brnd_desc AS prod_hier_l5,
		g.base_prod_desc AS prod_hier_l6,
		g.varnt_desc AS prod_hier_l7,
		g.put_up_desc AS prod_hier_l8,
		g.matl_desc AS prod_hier_l9,
		prod_map.prod_minor AS product_minor_code,
		prod_map.prod_minor_desc AS prod_minor_name,
		g.matl_num AS material_number,
		NULL AS ean,
		NULL AS retailer_sku_code,
		g.pka_product_key AS product_key,
		g.pka_product_key_description AS product_key_description,
		NULL AS target_value,
		NULL AS actual_value,
		SUM((a.jcp_qty * f.unt_prc::NUMERIC::NUMERIC(18, 0)) * exch_rate.ex_rt) AS value_usd,
		SUM(a.jcp_qty * f.unt_prc::NUMERIC::NUMERIC(18, 0)) AS value_lcy,
		0 AS salesweight,
		exch_rate.from_crncy AS from_crncy,
		exch_rate.from_crncy AS to_crncy,
		jcp_account AS account_number,
		key_figure_nm AS account_name,
		csw_category_1 AS account_description_l1,
		key_figure_nm_knj AS account_description_l2,
		key_figure_nm_dsp AS account_description_l3,
		NULL AS additional_information,
		NULL AS ppm_role
	FROM dm_integration_dly a -- Transaction
	LEFT JOIN edi_chn_m b -- Retailer Master
		ON a.jcp_chn_cd::TEXT = b.chn_cd::TEXT
	LEFT JOIN mt_account_key c -- Account Dimension
		ON a.jcp_account = c.accounting_code
	JOIN mt_cld e -- Calendar Dimension
		ON e.ymd_dt = a.jcp_date
	LEFT JOIN edi_item_m f -- Japan Product Master
		ON f.jan_cd_so::TEXT = a.jcp_jan_cd::TEXT
	-- Regional Product Master
	LEFT JOIN (
		SELECT matl_num,
			prodh1_txtmd,
			prodh2_txtmd,
			prodh3_txtmd,
			prodh4_txtmd,
			prodh5_txtmd,
			prodh6_txtmd,
			mega_brnd_desc,
			brnd_desc,
			base_prod_desc,
			varnt_desc,
			put_up_desc,
			matl_desc,
			prodh5,
			pka_product_key,
			pka_product_key_description
		FROM edw_material_dim
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
		) g ON LTRIM(g.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
	-- Product Plant
	LEFT JOIN (
		SELECT matl_num,
			prft_ctr
		FROM edw_material_plant_dim
		GROUP BY 1,
			2
		) mat_plant ON LTRIM(mat_plant.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
	LEFT JOIN edw_profit_center_franchise_mapping prod_map -- Profit Center
		ON LTRIM(mat_plant.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate -- Exchange Rates
		ON exch_rate.from_crncy::TEXT = (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'from_crncy'
				AND dataset_area = 'From Currency'
				AND dataset = 'Japan DMS'
			)
		AND exch_rate.to_crncy::TEXT = (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'to_crncy'
				AND dataset_area = 'To Currency'
				AND dataset = 'Japan DMS'
			)
		AND (((e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per::CHARACTER VARYING::TEXT
	LEFT JOIN v_intrm_disc_rebate_ytd dr_ytd ON (e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) = dr_ytd.fisc_yr_per::CHARACTER VARYING::TEXT
	WHERE a.jcp_data_source::TEXT IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'jcp_data_source'
				AND dataset_area = 'Sellout Data Source'
				AND dataset = 'Japan DMS'
			)
		AND jcp_chn_cd IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'jcp_chn_cd'
				AND dataset_area = 'GTS Parent Customer Filter'
				AND dataset = 'Japan DMS'
			)
	GROUP BY -- 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,50,51,52,53,54,55,56
		a.jcp_plan_type,
		e.year_445,
		e.year,
		e.month_445,
		e.month,
		to_date(e.ymd_dt),
		mat_plant.prft_ctr,
		a.jcp_chn_cd,
		-- Point to config table?
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END,
		b.lgl_nm,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END,
		prod_map.franchise_l1,
		prod_map.need_state,
		g.mega_brnd_desc,
		g.base_prod_desc,
		g.varnt_desc,
		g.put_up_desc,
		g.matl_desc,
		prod_map.prod_minor,
		prod_map.prod_minor_desc,
		g.matl_num,
		g.pka_product_key,
		g.pka_product_key_description,
		exch_rate.from_crncy,
		exch_rate.from_crncy,
		jcp_account,
		key_figure_nm,
		csw_category_1,
		key_figure_nm_knj,
		key_figure_nm_dsp
),
union2 as(
SELECT h.jcp_plan_type AS data_type,
		'JP_DMS' AS datasource,
		'SKU' AS data_level,
		'CIW' AS kpi,
		'Continuous' AS period_type,
		e.year_445 AS fisc_year,
		e.year AS cal_year,
		e.month_445::INTEGER AS fisc_month,
		e.month AS cal_month,
		to_date(e.ymd_dt) AS fisc_day,
		to_date(e.ymd_dt) AS cal_day,
		(e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) AS fisc_yr_per,
		'Japan' AS "cluster",
		'Japan' AS market,
		'Japan' AS sub_market,
		'E-Commerce' AS channel,
		'E-Commerce' AS sub_channel,
		'E-Commerce' AS retail_environment,
		'Indirect Accounts' AS go_to_model,
		mat_plant.prft_ctr AS profit_center,
		NULL AS company_code,
		h.jcp_chn_cd AS sap_customer_code,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END AS sap_customer_name,
		NULL AS banner,
		NULL AS banner_format,
		NULL AS platform_name,
		b.lgl_nm AS retailer_name,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		prod_map.franchise_l1 AS prod_hier_l1,
		prod_map.need_state AS prod_hier_l2,
		NULL AS prod_hier_l3,
		NULL AS prod_hier_l4,
		g.mega_brnd_desc AS prod_hier_l5,
		g.base_prod_desc AS prod_hier_l6,
		g.varnt_desc AS prod_hier_l7,
		g.put_up_desc AS prod_hier_l8,
		g.matl_desc AS prod_hier_l9,
		prod_map.prod_minor AS product_minor_code,
		prod_map.prod_minor_desc AS prod_minor_name,
		g.matl_num AS material_number,
		NULL AS ean,
		NULL AS retailer_sku_code,
		g.pka_product_key AS product_key,
		g.pka_product_key_description AS product_key_description,
		NULL AS target_value,
		NULL AS actual_value,
		- SUM(h.jcp_amt * exch_rate.ex_rt) AS value_usd,
		- SUM(h.jcp_amt) AS value_lcy,
		0 AS salesweight,
		exch_rate.from_crncy AS from_crncy,
		exch_rate.to_crncy AS to_crncy,
		NULL AS account_number,
		NULL AS account_name,
		NULL AS account_description_l1,
		NULL AS account_description_l2,
		NULL AS account_description_l3,
		NULL AS additional_information,
		NULL AS ppm_role
	FROM dm_integration_dly h -- Transactions
	JOIN mt_cld e -- Calendar Dimension
		ON to_date(h.jcp_date)::TIMESTAMP WITHOUT TIME ZONE = e.ymd_dt
	JOIN edi_chn_m b -- Retailer Master
		ON h.jcp_chn_offc_cd::TEXT = b.chn_offc_cd::TEXT
	LEFT JOIN edi_item_m f -- Japan Product Master
		ON f.jan_cd_so::TEXT = h.jcp_jan_cd::TEXT
	-- Regional Product Master
	LEFT JOIN (
		SELECT matl_num,
			prodh1_txtmd,
			prodh2_txtmd,
			prodh3_txtmd,
			prodh4_txtmd,
			prodh5_txtmd,
			prodh6_txtmd,
			mega_brnd_desc,
			brnd_desc,
			base_prod_desc,
			varnt_desc,
			put_up_desc,
			matl_desc,
			prodh5,
			pka_product_key,
			pka_product_key_description
		FROM edw_material_dim
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
		) g ON LTRIM(g.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
	-- Product Plant
	LEFT JOIN (
		SELECT matl_num,
			prft_ctr
		FROM edw_material_plant_dim
		GROUP BY 1,
			2
		) mat_plant ON LTRIM(mat_plant.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
	-- Profit Center
	LEFT JOIN edw_profit_center_franchise_mapping prod_map ON LTRIM(mat_plant.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
	-- TP Mapping Table
	LEFT JOIN mt_tp_status_mapping "map" ON "map".jcp_data_category::TEXT = h.jcp_data_category::TEXT
		AND "map".direct_flg = h.tp_apl_direct_flg::CHARACTER(2)
		AND "map".promo_status_cd = h.tp_promo_status_cd::CHARACTER(4)
		AND "map".approve_status_cd = h.tp_approve_status_cd::CHARACTER(4)
		AND "map".rslt_status_cd::TEXT = h.tp_rslt_status_cd::TEXT
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate -- Exchange Rates
		ON exch_rate.from_crncy::TEXT = (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'from_crncy'
				AND dataset_area = 'From Currency'
				AND dataset = 'Japan DMS'
			)
		AND exch_rate.to_crncy::TEXT = (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'to_crncy'
				AND dataset_area = 'To Currency'
				AND dataset = 'Japan DMS'
			)
		AND (((e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per::CHARACTER VARYING::TEXT
	LEFT JOIN v_intrm_disc_rebate_ytd dr_ytd ON (e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) = dr_ytd.fisc_yr_per::CHARACTER VARYING::TEXT
	WHERE h.jcp_data_source::TEXT IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'jcp_data_source'
				AND dataset_area = 'TP Data Source'
				AND dataset = 'Japan DMS'
			)
		AND "map".mapping_status_nm1::TEXT NOT IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'mapping_status_nm1'
				AND dataset_area = 'TP Mapping Status Name'
				AND dataset = 'Japan DMS'
			)
		AND b.lgl_nm::TEXT != '一般（TP）%'::CHARACTER VARYING::TEXT
		AND b.lgl_nm IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE column_name = 'lgl_nm'
				AND dataset_area = 'TP Parent Customer Filter'
				AND dataset = 'Japan DMS'
			)
	GROUP BY h.jcp_plan_type,
		e.year_445,
		e.year,
		e.month_445,
		e.month,
		to_date(e.ymd_dt),
		mat_plant.prft_ctr,
		h.jcp_chn_cd,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END,
		b.lgl_nm,
		CASE 
			WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
				THEN 'ECKA'
			WHEN b.lgl_nm = 'アマゾンジャパン'
				THEN 'AMAZON'
			WHEN b.lgl_nm = 'アスクル'
				THEN 'LOHACO'
			WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
				THEN 'RAKUTEN D'
			WHEN b.lgl_nm = '楽天株式会社'
				THEN 'RAKUTEN F'
			ELSE b.lgl_nm
			END,
		prod_map.franchise_l1,
		prod_map.need_state,
		g.mega_brnd_desc,
		g.base_prod_desc,
		g.varnt_desc,
		g.put_up_desc,
		g.matl_desc,
		prod_map.prod_minor,
		prod_map.prod_minor_desc,
		g.matl_num,
		g.pka_product_key,
		g.pka_product_key_description,
		exch_rate.from_crncy,
		exch_rate.to_crncy
),
derived_table as(
	select * from union1
	UNION ALL
	select * from union2	
),
insert5 as(
SELECT derived_table.data_type,
	derived_table.datasource,
	derived_table.data_level,
	'NTS' AS kpi,
	derived_table.period_type,
	derived_table.fisc_year,
	derived_table.cal_year,
	derived_table.fisc_month,
	derived_table.cal_month,
	derived_table.fisc_day,
	derived_table.cal_day,
	derived_table.fisc_yr_per,
	derived_table."cluster",
	derived_table.market,
	derived_table.sub_market,
	derived_table.channel,
	derived_table.sub_channel,
	derived_table.retail_environment,
	derived_table.go_to_model,
	derived_table.profit_center,
	derived_table.company_code,
	derived_table.sap_customer_code,
	derived_table.sap_customer_name,
	derived_table.banner,
	derived_table.banner_format,
	derived_table.platform_name,
	derived_table.retailer_name,
	derived_table.retailer_name_english,
	derived_table.manufacturer_name,
	derived_table.jj_manufacturer_flag,
	derived_table.prod_hier_l1,
	derived_table.prod_hier_l2,
	derived_table.prod_hier_l3,
	derived_table.prod_hier_l4,
	derived_table.prod_hier_l5,
	derived_table.prod_hier_l6,
	derived_table.prod_hier_l7,
	derived_table.prod_hier_l8,
	derived_table.prod_hier_l9,
	derived_table.product_minor_code,
	derived_table.prod_minor_name,
	derived_table.material_number,
	derived_table.ean,
	derived_table.retailer_sku_code,
	derived_table.product_key,
	derived_table.product_key_description,
	derived_table.target_value,
	derived_table.actual_value,
	SUM(value_usd) AS value_usd,
	SUM(value_lcy) AS value_lcy,
	0 AS salesweight,
	derived_table.from_crncy,
	derived_table.to_crncy,
	derived_table.account_number,
	derived_table.account_name,
	derived_table.account_description_l1,
	derived_table.account_description_l2,
	derived_table.account_description_l3,
	derived_table.additional_information,
	derived_table.ppm_role
FROM derived_table
WHERE fisc_year >= EXTRACT(YEAR FROM GETDATE()) - 2 -- Limit to latest 3 years of data
	--fisc_year = 2021 AND fisc_month IN (1,2,3,4,5,6)
GROUP BY --1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,50,51,52,53,54,55,56,57,58;
	derived_table.data_type,
	derived_table.datasource,
	derived_table.data_level,
	derived_table.period_type,
	derived_table.fisc_year,
	derived_table.cal_year,
	derived_table.fisc_month,
	derived_table.cal_month,
	derived_table.fisc_day,
	derived_table.cal_day,
	derived_table.fisc_yr_per,
	derived_table."cluster",
	derived_table.market,
	derived_table.sub_market,
	derived_table.channel,
	derived_table.sub_channel,
	derived_table.retail_environment,
	derived_table.go_to_model,
	derived_table.profit_center,
	derived_table.company_code,
	derived_table.sap_customer_code,
	derived_table.sap_customer_name,
	derived_table.banner,
	derived_table.banner_format,
	derived_table.platform_name,
	derived_table.retailer_name,
	derived_table.retailer_name_english,
	derived_table.manufacturer_name,
	derived_table.jj_manufacturer_flag,
	derived_table.prod_hier_l1,
	derived_table.prod_hier_l2,
	derived_table.prod_hier_l3,
	derived_table.prod_hier_l4,
	derived_table.prod_hier_l5,
	derived_table.prod_hier_l6,
	derived_table.prod_hier_l7,
	derived_table.prod_hier_l8,
	derived_table.prod_hier_l9,
	derived_table.product_minor_code,
	derived_table.prod_minor_name,
	derived_table.material_number,
	derived_table.ean,
	derived_table.retailer_sku_code,
	derived_table.product_key,
	derived_table.product_key_description,
	derived_table.target_value,
	derived_table.actual_value,
	derived_table.from_crncy,
	derived_table.to_crncy,
	derived_table.account_number,
	derived_table.account_name,
	derived_table.account_description_l1,
	derived_table.account_description_l2,
	derived_table.account_description_l3,
	derived_table.additional_information,
	derived_table.ppm_role
),
insert6_union1 as(
SELECT h.jcp_plan_type AS data_type,
	'JP_DMS' AS datasource,
	'SKU' AS data_level,
	'CIW' AS kpi,
	'Continuous' AS period_type,
	e.year_445 AS fisc_year,
	e.year AS cal_year,
	e.month_445::INTEGER AS fisc_month,
	e.month AS cal_month,
	to_date(e.ymd_dt) AS fisc_day,
	to_date(e.ymd_dt) AS cal_day,
	(e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) AS fisc_yr_per,
	'Japan' AS "cluster",
	'Japan' AS market,
	'Japan' AS sub_market,
	'E-Commerce' AS channel,
	'E-Commerce' AS sub_channel,
	'E-Commerce' AS retail_environment,
	'Indirect Accounts' AS go_to_model,
	mat_plant.prft_ctr AS profit_center,
	NULL AS company_code,
	h.jcp_chn_cd AS sap_customer_code,
	-- Point to config table?
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END AS sap_customer_name,
	NULL AS banner,
	NULL AS banner_format,
	NULL AS platform_name,
	b.lgl_nm AS retailer_name,
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END AS retailer_name_english,
	'Johnson & Johnson' AS manufacturer_name,
	'Y' AS jj_manufacturer_flag,
	prod_map.franchise_l1 AS prod_hier_l1,
	prod_map.need_state AS prod_hier_l2,
	NULL AS prod_hier_l3,
	NULL AS prod_hier_l4,
	g.mega_brnd_desc AS prod_hier_l5,
	g.base_prod_desc AS prod_hier_l6,
	g.varnt_desc AS prod_hier_l7,
	g.put_up_desc AS prod_hier_l8,
	g.matl_desc AS prod_hier_l9,
	prod_map.prod_minor AS product_minor_code,
	prod_map.prod_minor_desc AS prod_minor_name,
	g.matl_num AS material_number,
	NULL AS ean,
	NULL AS retailer_sku_code,
	g.pka_product_key AS product_key,
	g.pka_product_key_description AS product_key_description,
	NULL AS target_value,
	NULL AS actual_value,
	- SUM(h.jcp_amt * exch_rate.ex_rt) AS value_usd,
	- SUM(h.jcp_amt) AS value_lcy,
	0 AS salesweight,
	exch_rate.from_crncy AS from_crncy,
	exch_rate.to_crncy AS to_crncy,
	jcp_account AS account_number,
	key_figure_nm AS account_name,
	csw_category_1 AS account_description_l1,
	key_figure_nm_knj AS account_description_l2,
	key_figure_nm_dsp AS account_description_l3,
	NULL AS additional_information,
	NULL AS ppm_role
FROM dm_integration_dly h -- Transactions
JOIN mt_cld e -- Calendar Dimension
	ON to_date(h.jcp_date)::TIMESTAMP WITHOUT TIME ZONE = e.ymd_dt
JOIN edi_chn_m b -- Retailer Master
	ON h.jcp_chn_offc_cd::TEXT = b.chn_offc_cd::TEXT
--LEFT JOIN edi_chn_m b -- Retailer Master
--ON h.jcp_chn_cd::TEXT = b.chn_cd::TEXT
LEFT JOIN mt_account_key c -- Account Dimension
	ON h.jcp_account = c.accounting_code
--JOIN mt_sgmt c ON b.sgmt::TEXT = c.sgmt::TEXT
LEFT JOIN edi_item_m f -- Japan Product Master
	ON f.jan_cd_so::TEXT = h.jcp_jan_cd::TEXT
-- Regional Product Master
LEFT JOIN (
	SELECT matl_num,
		prodh1_txtmd,
		prodh2_txtmd,
		prodh3_txtmd,
		prodh4_txtmd,
		prodh5_txtmd,
		prodh6_txtmd,
		mega_brnd_desc,
		brnd_desc,
		base_prod_desc,
		varnt_desc,
		put_up_desc,
		matl_desc,
		prodh5,
		pka_product_key,
		pka_product_key_description
	FROM edw_material_dim
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
	) g ON LTRIM(g.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
-- Product Plant
LEFT JOIN (
	SELECT matl_num,
		prft_ctr
	FROM edw_material_plant_dim
	GROUP BY 1,
		2
	) mat_plant ON LTRIM(mat_plant.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
-- Profit Center
LEFT JOIN edw_profit_center_franchise_mapping prod_map ON LTRIM(mat_plant.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
-- TP Mapping Table
LEFT JOIN mt_tp_status_mapping "map" ON "map".jcp_data_category::TEXT = h.jcp_data_category::TEXT
	AND "map".direct_flg = h.tp_apl_direct_flg::CHARACTER(2)
	AND "map".promo_status_cd = h.tp_promo_status_cd::CHARACTER(4)
	AND "map".approve_status_cd = h.tp_approve_status_cd::CHARACTER(4)
	AND "map".rslt_status_cd::TEXT = h.tp_rslt_status_cd::TEXT
LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate -- Exchange Rates
	ON exch_rate.from_crncy::TEXT = (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'from_crncy'
			AND dataset_area = 'From Currency'
			AND dataset = 'Japan DMS'
		)
	AND exch_rate.to_crncy::TEXT = (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'to_crncy'
			AND dataset_area = 'To Currency'
			AND dataset = 'Japan DMS'
		)
	AND (((e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per::CHARACTER VARYING::TEXT
LEFT JOIN v_intrm_disc_rebate_ytd dr_ytd ON (e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) = dr_ytd.fisc_yr_per::CHARACTER VARYING::TEXT
WHERE h.jcp_data_source::TEXT IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_data_source'
			AND dataset_area = 'TP Data Source'
			AND dataset = 'Japan DMS'
		)
	AND "map".mapping_status_nm1::TEXT NOT IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'mapping_status_nm1'
			AND dataset_area = 'TP Mapping Status Name'
			AND dataset = 'Japan DMS'
		)
	AND b.lgl_nm::TEXT != '一般（TP）%'::CHARACTER VARYING::TEXT
	AND b.lgl_nm IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'lgl_nm'
			AND dataset_area = 'TP Parent Customer Filter'
			AND dataset = 'Japan DMS'
		)
GROUP BY --1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,50,51,52,53,54,55,56
	h.jcp_plan_type,
	e.year_445,
	e.year,
	e.month_445,
	e.month,
	to_date(e.ymd_dt),
	mat_plant.prft_ctr,
	h.jcp_chn_cd,
	-- Point to config table?
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END,
	b.lgl_nm,
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END,
	prod_map.franchise_l1,
	prod_map.need_state,
	g.mega_brnd_desc,
	g.base_prod_desc,
	g.varnt_desc,
	g.put_up_desc,
	g.matl_desc,
	prod_map.prod_minor,
	prod_map.prod_minor_desc,
	g.matl_num,
	g.pka_product_key,
	g.pka_product_key_description,
	exch_rate.from_crncy,
	exch_rate.to_crncy,
	jcp_account,
	key_figure_nm,
	csw_category_1,
	key_figure_nm_knj,
	key_figure_nm_dsp
),
-- Returns 
insert6_union2 as(
SELECT a.jcp_plan_type AS data_type,
	'JP_DMS' AS datasource,
	'SKU' AS data_level,
	'CIW' AS kpi,
	'Continuous' AS period_type,
	e.year_445 AS fisc_year,
	e.year AS cal_year,
	e.month_445::INTEGER AS fisc_month,
	e.month AS cal_month,
	to_date(e.ymd_dt) AS fisc_day,
	to_date(e.ymd_dt) AS cal_day,
	(e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) AS fisc_yr_per,
	'Japan' AS "cluster",
	'Japan' AS market,
	'Japan' AS sub_market,
	'E-Commerce' AS channel,
	'E-Commerce' AS sub_channel,
	'E-Commerce' AS retail_environment,
	'Indirect Accounts' AS go_to_model,
	mat_plant.prft_ctr AS profit_center,
	'Not Available' AS company_code,
	a.jcp_chn_cd AS sap_customer_code,
	-- Point to config table?
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END AS sap_customer_name,
	NULL AS banner,
	NULL AS banner_format,
	NULL AS platform_name,
	b.lgl_nm AS retailer_name,
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END AS retailer_name_english,
	'Johnson & Johnson' AS manufacturer_name,
	'Y' AS jj_manufacturer_flag,
	prod_map.franchise_l1 AS prod_hier_l1,
	prod_map.need_state AS prod_hier_l2,
	NULL AS prod_hier_l3,
	NULL AS prod_hier_l4,
	g.mega_brnd_desc AS prod_hier_l5,
	g.base_prod_desc AS prod_hier_l6,
	g.varnt_desc AS prod_hier_l7,
	g.put_up_desc AS prod_hier_l8,
	g.matl_desc AS prod_hier_l9,
	prod_map.prod_minor AS product_minor_code,
	prod_map.prod_minor_desc AS prod_minor_name,
	g.matl_num AS material_number,
	NULL AS ean,
	NULL AS retailer_sku_code,
	g.pka_product_key AS product_key,
	g.pka_product_key_description AS product_key_description,
	NULL AS target_value,
	NULL AS actual_value,
	SUM((a.jcp_qty * f.unt_prc::NUMERIC::NUMERIC(18, 0)) * exch_rate.ex_rt) AS value_usd,
	SUM(a.jcp_qty * f.unt_prc::NUMERIC::NUMERIC(18, 0)) AS value_lcy,
	0 AS salesweight,
	exch_rate.from_crncy AS from_crncy,
	exch_rate.from_crncy AS to_crncy,
	jcp_account AS account_number,
	key_figure_nm AS account_name,
	csw_category_1 AS account_description_l1,
	key_figure_nm_knj AS account_description_l2,
	key_figure_nm_dsp AS account_description_l3,
	NULL AS additional_information,
	NULL AS ppm_role
FROM dm_integration_dly a -- Transaction
LEFT JOIN edi_chn_m b -- Retailer Master
	ON a.jcp_chn_cd::TEXT = b.chn_cd::TEXT
LEFT JOIN mt_account_key c -- Account Dimension
	ON a.jcp_account = c.accounting_code
JOIN mt_cld e -- Calendar Dimension
	ON e.ymd_dt = a.jcp_date
LEFT JOIN edi_item_m f -- Japan Product Master
	ON f.jan_cd_so::TEXT = a.jcp_jan_cd::TEXT
-- Regional Product Master
LEFT JOIN (
	SELECT matl_num,
		prodh1_txtmd,
		prodh2_txtmd,
		prodh3_txtmd,
		prodh4_txtmd,
		prodh5_txtmd,
		prodh6_txtmd,
		mega_brnd_desc,
		brnd_desc,
		base_prod_desc,
		varnt_desc,
		put_up_desc,
		matl_desc,
		prodh5,
		pka_product_key,
		pka_product_key_description
	FROM edw_material_dim
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
	) g ON LTRIM(g.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
-- Product Plant
LEFT JOIN (
	SELECT matl_num,
		prft_ctr
	FROM edw_material_plant_dim
	GROUP BY 1,
		2
	) mat_plant ON LTRIM(mat_plant.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
LEFT JOIN edw_profit_center_franchise_mapping prod_map -- Profit Center
	ON LTRIM(mat_plant.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate -- Exchange Rates
	ON exch_rate.from_crncy::TEXT = (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'from_crncy'
			AND dataset_area = 'From Currency'
			AND dataset = 'Japan DMS'
		)
	AND exch_rate.to_crncy::TEXT = (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'to_crncy'
			AND dataset_area = 'To Currency'
			AND dataset = 'Japan DMS'
		)
	AND (((e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per::CHARACTER VARYING::TEXT
LEFT JOIN v_intrm_disc_rebate_ytd dr_ytd ON (e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) = dr_ytd.fisc_yr_per::CHARACTER VARYING::TEXT
WHERE a.jcp_data_source::TEXT IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_data_source'
			AND dataset_area = 'Sellout Data Source'
			AND dataset = 'Japan DMS'
		)
	AND jcp_chn_cd IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_chn_cd'
			AND dataset_area = 'GTS Parent Customer Filter'
			AND dataset = 'Japan DMS'
		)
	AND jcp_account IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_account'
			AND dataset_area = 'Account Inclusion'
			AND dataset = 'Japan DMS (CIW)'
		)
GROUP BY a.jcp_plan_type,
	e.year_445,
	e.year,
	e.month_445,
	e.month,
	e.ymd_dt,
	mat_plant.prft_ctr,
	a.jcp_chn_cd,
	-- Point to config table?
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END,
	b.lgl_nm,
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END,
	prod_map.franchise_l1,
	prod_map.need_state,
	g.mega_brnd_desc,
	g.base_prod_desc,
	g.varnt_desc,
	g.put_up_desc,
	g.matl_desc,
	prod_map.prod_minor,
	prod_map.prod_minor_desc,
	g.matl_num,
	g.pka_product_key,
	g.pka_product_key_description,
	exch_rate.from_crncy,
	exch_rate.from_crncy,
	jcp_account,
	key_figure_nm,
	csw_category_1,
	key_figure_nm_knj,
	key_figure_nm_dsp
),
insert6 as(
	select * from insert6_union1
	union all
	select * from insert6_union2
),
insert7 as(
	SELECT a.jcp_plan_type AS data_type,
	'JP_DMS' AS datasource,
	'SKU' AS data_level,
	'GTS' AS kpi,
	'Continuous' AS period_type,
	e.year_445 AS fisc_year,
	e.year AS cal_year,
	e.month_445::INTEGER AS fisc_month,
	e.month AS cal_month,
	to_date(e.ymd_dt) AS fisc_day,
	to_date(e.ymd_dt) AS cal_day,
	(e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) AS fisc_yr_per,
	'Japan' AS "cluster",
	'Japan' AS market,
	'Japan' AS sub_market,
	'E-Commerce' AS channel,
	'E-Commerce' AS sub_channel,
	'E-Commerce' AS retail_environment,
	'Indirect Accounts' AS go_to_model,
	mat_plant.prft_ctr AS profit_center,
	'Not Available' AS company_code,
	a.jcp_chn_cd AS sap_customer_code,
	-- Point to config table?
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END AS sap_customer_name,
	NULL AS banner,
	NULL AS banner_format,
	NULL AS platform_name,
	b.lgl_nm AS retailer_name,
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END AS retailer_name_english,
	'Johnson & Johnson' AS manufacturer_name,
	'Y' AS jj_manufacturer_flag,
	prod_map.franchise_l1 AS prod_hier_l1,
	prod_map.need_state AS prod_hier_l2,
	NULL AS prod_hier_l3,
	NULL AS prod_hier_l4,
	g.mega_brnd_desc AS prod_hier_l5,
	g.base_prod_desc AS prod_hier_l6,
	g.varnt_desc AS prod_hier_l7,
	g.put_up_desc AS prod_hier_l8,
	g.matl_desc AS prod_hier_l9,
	prod_map.prod_minor AS product_minor_code,
	prod_map.prod_minor_desc AS prod_minor_name,
	g.matl_num AS material_number,
	NULL AS ean,
	NULL AS retailer_sku_code,
	g.pka_product_key AS product_key,
	g.pka_product_key_description AS product_key_description,
	NULL AS target_value,
	NULL AS actual_value,
	SUM((a.jcp_qty * f.unt_prc::NUMERIC::NUMERIC(18, 0)) * exch_rate.ex_rt) AS value_usd,
	SUM(a.jcp_qty * f.unt_prc::NUMERIC::NUMERIC(18, 0)) AS value_lcy,
	0 AS salesweight,
	exch_rate.from_crncy AS from_crncy,
	exch_rate.from_crncy AS to_crncy,
	jcp_account AS account_number,
	key_figure_nm AS account_name,
	csw_category_1 AS account_description_l1,
	key_figure_nm_knj AS account_description_l2,
	key_figure_nm_dsp AS account_description_l3,
	NULL AS additional_information,
	NULL AS ppm_role
FROM dm_integration_dly a -- Transaction
LEFT JOIN edi_chn_m b -- Retailer Master
	ON a.jcp_chn_cd::TEXT = b.chn_cd::TEXT
LEFT JOIN mt_account_key c -- Account Dimension
	ON a.jcp_account = c.accounting_code
JOIN mt_cld e -- Calendar Dimension
	ON e.ymd_dt = a.jcp_date
LEFT JOIN edi_item_m f -- Japan Product Master
	ON f.jan_cd_so::TEXT = a.jcp_jan_cd::TEXT
-- Regional Product Master
LEFT JOIN (
	SELECT matl_num,
		prodh1_txtmd,
		prodh2_txtmd,
		prodh3_txtmd,
		prodh4_txtmd,
		prodh5_txtmd,
		prodh6_txtmd,
		mega_brnd_desc,
		brnd_desc,
		base_prod_desc,
		varnt_desc,
		put_up_desc,
		matl_desc,
		prodh5,
		pka_product_key,
		pka_product_key_description
	FROM edw_material_dim
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
	) g ON LTRIM(g.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
-- Product Plant
LEFT JOIN (
	SELECT matl_num,
		prft_ctr
	FROM edw_material_plant_dim
	GROUP BY 1,
		2
	) mat_plant ON LTRIM(mat_plant.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) = f.item_cd::TEXT
LEFT JOIN edw_profit_center_franchise_mapping prod_map -- Profit Center
	ON LTRIM(mat_plant.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate -- Exchange Rates
	ON exch_rate.from_crncy::TEXT = (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'from_crncy'
			AND dataset_area = 'From Currency'
			AND dataset = 'Japan DMS'
		)
	AND exch_rate.to_crncy::TEXT = (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'to_crncy'
			AND dataset_area = 'To Currency'
			AND dataset = 'Japan DMS'
		)
	AND (((e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT))::CHARACTER VARYING)::TEXT) = exch_rate.fisc_per::CHARACTER VARYING::TEXT
LEFT JOIN v_intrm_disc_rebate_ytd dr_ytd ON (e.year_445::TEXT || LPAD(e.month_445::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) = dr_ytd.fisc_yr_per::CHARACTER VARYING::TEXT
WHERE a.jcp_data_source::TEXT IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_data_source'
			AND dataset_area = 'Sellout Data Source'
			AND dataset = 'Japan DMS'
		)
	AND jcp_chn_cd IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_chn_cd'
			AND dataset_area = 'GTS Parent Customer Filter'
			AND dataset = 'Japan DMS'
		)
	AND jcp_account IN (
		SELECT filter_value
		FROM itg_mds_ap_ecom_oneview_config
		WHERE column_name = 'jcp_account'
			AND dataset_area = 'Account Inclusion'
			AND dataset = 'Japan DMS (GTS)'
		)
GROUP BY a.jcp_plan_type,
	e.year_445,
	e.year,
	e.month_445,
	e.month,
	e.ymd_dt,
	mat_plant.prft_ctr,
	a.jcp_chn_cd,
	-- Point to config table?
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END,
	b.lgl_nm,
	CASE 
		WHEN b.lgl_nm IN ('ヨドバシカメラ', 'カウネット', '大塚商会', 'ａｕコマース＆ライフ')
			THEN 'ECKA'
		WHEN b.lgl_nm = 'アマゾンジャパン'
			THEN 'AMAZON'
		WHEN b.lgl_nm = 'アスクル'
			THEN 'LOHACO'
		WHEN b.lgl_nm IN ('爽快ドラッグ', 'ケンコーコム')
			THEN 'RAKUTEN D'
		WHEN b.lgl_nm = '楽天株式会社'
			THEN 'RAKUTEN F'
		ELSE b.lgl_nm
		END,
	prod_map.franchise_l1,
	prod_map.need_state,
	g.mega_brnd_desc,
	g.base_prod_desc,
	g.varnt_desc,
	g.put_up_desc,
	g.matl_desc,
	prod_map.prod_minor,
	prod_map.prod_minor_desc,
	g.matl_num,
	g.pka_product_key,
	g.pka_product_key_description,
	exch_rate.from_crncy,
	exch_rate.from_crncy,
	jcp_account,
	key_figure_nm,
	csw_category_1,
	key_figure_nm_knj,
	key_figure_nm_dsp
),
insert8_union1 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
       'Manual NTS' as Datasource,
       'SKU' as Data_level,
       'NTS' as KPI,		   
       'Monthly' as Period_type,
       apac_ecomm_nts.year::CHARACTER VARYING AS fisc_year,
	   null as cal_year,
       apac_ecomm_nts.month::CHARACTER VARYING AS fisc_month,
	   null as cal_month, 
       TO_DATE((LPAD(apac_ecomm_nts.month::CHARACTER VARYING::TEXT,2,'0'::CHARACTER VARYING::TEXT) || '01'::CHARACTER VARYING::TEXT) || apac_ecomm_nts.year::CHARACTER VARYING::TEXT,'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
       null ::date as cal_day,
       (apac_ecomm_nts.year::CHARACTER VARYING::TEXT || LPAD(apac_ecomm_nts.month::CHARACTER VARYING::TEXT,2,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING::TEXT)::INTEGER AS fisc_yr_per,
       CASE
         WHEN apac_ecomm_nts.country::TEXT = 'Pacific'::CHARACTER VARYING::TEXT THEN 'Pacific'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'China Selfcare'::CHARACTER VARYING::TEXT THEN 'China'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'China Omni'::CHARACTER VARYING::TEXT THEN 'China'::CHARACTER VARYING
		 WHEN apac_ecomm_nts.country = 'China Selfcare (O2O)' THEN 'China'
         WHEN apac_ecomm_nts.country::TEXT = 'Korea Omni'::CHARACTER VARYING::TEXT THEN 'Metropolitan Asia'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'Vietnam'::CHARACTER VARYING::TEXT THEN 'Metropolitan Asia'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'Japan DCL'::CHARACTER VARYING::TEXT THEN 'Japan'::CHARACTER VARYING
         ELSE NULL::CHARACTER VARYING
       END AS "cluster",
       CASE
         WHEN apac_ecomm_nts.country::TEXT = 'Pacific'::CHARACTER VARYING::TEXT THEN 'Pacific'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'China Selfcare'::CHARACTER VARYING::TEXT THEN 'China Selfcare'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'China Omni'::CHARACTER VARYING::TEXT THEN 'China Personal Care'::CHARACTER VARYING
		 WHEN apac_ecomm_nts.country = 'China Selfcare (O2O)' THEN 'China Selfcare' 
         WHEN apac_ecomm_nts.country::TEXT = 'Korea Omni'::CHARACTER VARYING::TEXT THEN 'Korea'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'Vietnam'::CHARACTER VARYING::TEXT THEN 'Vietnam'::CHARACTER VARYING
         WHEN apac_ecomm_nts.country::TEXT = 'Japan DCL'::CHARACTER VARYING::TEXT THEN 'Japan DCL'::CHARACTER VARYING
         ELSE NULL::CHARACTER VARYING
       END AS ctry_nm,
       apac_ecomm_nts.country AS sub_country,
       'na' AS channel,
       'na' AS "sub channel",
       'na' AS retail_env,
       'Indirect Accounts' AS "go to model",
       'na' AS profit_center, 	   
       'na' AS company_code,
       'na' AS customer_code,
       apac_ecomm_nts.customer_name AS "parent customer",
       'na' AS banner,
       'na' AS "banner format",
	   null as platform_name,
	   null as retailer_name,
	   null as retailer_name_english,
	   'Johnson & Johnson' as manufacturer_name,
	   'Y' as jj_manufacturer_flag,
       apac_ecomm_nts.gfo AS franchise_l1,
       apac_ecomm_nts.need_state,
	   'na' as prod_hier_l3,
	   'na' as prod_hier_l4,
       'na' AS "b1 mega-brand",
       apac_ecomm_nts.brand AS "b2 brand",
      'na' AS "b4 variant",
      'na' AS "b5 put-up",
      'na' AS sku,
       'na' AS product_minor_code,	
       'na' AS prod_minor,   
       'na' AS product_code,       
 	   null as ean,
	   null as reailer_sku_code,
       'na' AS pka_product_key,
       'na' AS pka_product_key_description,
       0 AS target_ori,
       0 AS value,	 
       CASE
         WHEN apac_ecomm_nts.country::TEXT = 'Korea Omni'::CHARACTER VARYING::TEXT THEN apac_ecomm_nts.nts_usd / apac_ecomm_nts.ex_rt_to_usd*iqp.parameter_value::DOUBLE precision*exch_rate.ex_rt::DOUBLE precision
         ELSE apac_ecomm_nts.nts_usd / apac_ecomm_nts.ex_rt_to_usd*exch_rate.ex_rt::DOUBLE precision
       END AS usd_value,
       CASE
         WHEN apac_ecomm_nts.country::TEXT = 'Korea Omni'::CHARACTER VARYING::TEXT THEN apac_ecomm_nts.nts_usd / apac_ecomm_nts.ex_rt_to_usd*iqp.parameter_value::DOUBLE precision
         ELSE apac_ecomm_nts.nts_usd / apac_ecomm_nts.ex_rt_to_usd
       END AS lcy_value, 
	   0 as salesweight,	   
       exch_rate.from_crncy,
       exch_rate.to_crncy,	
       'na' AS acct_nm,
       'na' AS acct_num,
       'na' AS ciw_desc,
       'na' AS ciw_bucket,
       'na' AS csw_desc,
       'na' AS "Additional_Information"	,
       NULL as ppm_role   
	FROM edw_ecommerce_nts_regional apac_ecomm_nts
	LEFT JOIN itg_query_parameters iqp ON UPPER (apac_ecomm_nts.country::TEXT) = iqp.parameter_name::TEXT
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
			ON apac_ecomm_nts.from_crncy::TEXT = exch_rate.from_crncy::TEXT
			AND exch_rate.to_crncy::TEXT = ( select  filter_value 
													from itg_mds_ap_ecom_oneview_config        
													where dataset_area = 'To Currency' 
													and column_name = 'to_crncy'
													and dataset = 'Manual NTS Submission' )
			AND exch_rate.fisc_per::CHARACTER VARYING::TEXT = (apac_ecomm_nts.year::CHARACTER VARYING::TEXT || LPAD (apac_ecomm_nts.month::CHARACTER VARYING::TEXT,3,'0'::CHARACTER VARYING::TEXT))
	WHERE apac_ecomm_nts.year  >= ( select (select fisc_yr 
												from edw_calendar_dim 
												where cal_day = to_date(current_timestamp())-365) - filter_value  as filter_value 
										from itg_mds_ap_ecom_oneview_config        
										where column_name = 'year'
										and dataset_area = 'Year Range'
										and dataset = 'Manual NTS Submission' )and upper(ctry_nm)<>'KOREA'
),
insert8_union2 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
       'Manual NTS' as Datasource,
       'SKU' as Data_level,
	   'NTS' as KPI,	
       'Monthly' as Period_type,
       ecomm_nts_manual_adj.fisc_year,
	   null as cal_year,
       ecomm_nts_manual_adj.fisc_month,
	   null as cal_month,   
       TO_DATE((LPAD(ecomm_nts_manual_adj.fisc_month::TEXT,2,'0'::CHARACTER VARYING::TEXT) || '01'::CHARACTER VARYING::TEXT) || ecomm_nts_manual_adj.fisc_year::TEXT,'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
	   null as cal_day,
       (ecomm_nts_manual_adj.fisc_year::TEXT || LPAD(ecomm_nts_manual_adj.fisc_month::TEXT,2,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING::TEXT)::INTEGER AS fisc_yr_per,
       ecomm_nts_manual_adj.cluster,
       ecomm_nts_manual_adj.country AS ctry_nm,
       ecomm_nts_manual_adj.sub_country,       
       'na' AS channel,
       'na' AS "sub channel",
       'na' AS retail_env,
       'Indirect Accounts' AS "go to model",
       'na' AS profit_center, 	   
       'na' AS company_code,
       'na' AS customer_code,
       ecomm_nts_manual_adj.customer_name AS "parent customer",
       'na' AS banner,
       'na' AS "banner format",
	   null as platform_name,
	   null as retailer_name,
	   null as retailer_name_english,
	   'Johnson & Johnson' as manufacturer_name,
	   'Y' as jj_manufacturer_flag,
       ecomm_nts_manual_adj.gfo AS franchise_l1,
       ecomm_nts_manual_adj.need_state,  
	   'na' as prod_hier_l3,
	   'na' as prod_hier_l4,
       'na' AS "b1 mega-brand",
       ecomm_nts_manual_adj.brand AS "b2 brand",
      'na' AS "b4 variant",
      'na' AS "b5 put-up",
      'na' AS sku,
       'na' AS product_minor_code,	
       'na' AS prod_minor,   
       'na' AS product_code,       
 	   null as ean,
	   null as reailer_sku_code,
       'na' AS pka_product_key,
       'na' AS pka_product_key_description,
       0 AS target_ori,
       0 AS value,	   
       ecomm_nts_manual_adj.nts_usd / ecomm_nts_manual_adj.ex_rt_to_usd*exch_rate.ex_rt::DOUBLE precision AS usd_value,
       ecomm_nts_manual_adj.nts_usd / ecomm_nts_manual_adj.ex_rt_to_usd AS lcy_value,
 0 as salesweight,
       exch_rate.from_crncy,
       exch_rate.to_crncy,
       'na' AS acct_nm,
       'na' AS acct_num,
       'na' AS ciw_desc,
       'na' AS ciw_bucket,
       'na' AS csw_desc,
       'na' AS "Additional_Information"	,
       NULL as ppm_role   
	FROM edw_ap_ecomm_nts_manual_adjustment ecomm_nts_manual_adj
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
			ON ecomm_nts_manual_adj.from_crncy::TEXT = exch_rate.from_crncy::TEXT
			AND exch_rate.to_crncy::TEXT = ( select  filter_value 
													from itg_mds_ap_ecom_oneview_config        
													where dataset_area = 'To Currency' 
													and column_name = 'to_crncy'
													and dataset = 'Manual NTS Adjustment' )
			AND exch_rate.fisc_per::CHARACTER VARYING::TEXT = (ecomm_nts_manual_adj.fisc_year::TEXT || LPAD (ecomm_nts_manual_adj.fisc_month::TEXT,3,'0'::CHARACTER VARYING::TEXT)) where upper(ctry_nm)<>'KOREA'
),
insert8_union3 as(
	
	SELECT 'Tgt'::CHARACTER VARYING AS data_type,
		'ECOMM Plan' as Datasource,
		'SKU' as Data_level,
		'Target_NTS' as KPI,	 	   
		'Monthly' as Period_type,
		ecomm.fisc_year::CHARACTER VARYING AS fisc_year,
		null as cal_year,
		ecomm.fisc_month::CHARACTER VARYING AS fisc_month,
		null as cal_month,
		-- ecomm.fisc_quarter,
		TO_DATE((LPAD(ecomm.fisc_month::CHARACTER VARYING::TEXT,2,'0'::CHARACTER VARYING::TEXT) || '01'::CHARACTER VARYING::TEXT) || ecomm.fisc_year::CHARACTER VARYING::TEXT,'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		null as cal_day,
		(ecomm.fisc_year::CHARACTER VARYING::TEXT || LPAD(ecomm.fisc_month::CHARACTER VARYING::TEXT,2,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING::TEXT)::INTEGER AS fisc_yr_per,
		ecomm.cluster,
		ecomm.country AS ctry_nm,
		ecomm.sub_country,    
		'na' AS channel,
		'na' AS "sub channel",
		'na' AS retail_env,
		'na' AS "go to model",
		'na' AS profit_center, 	   
		'na' AS company_code,
		'na' AS customer_code,
		'na' AS "parent customer",
		'na' AS banner,
		'na' AS "banner format",
		null as platform_name,
		null as retailer_name,
		null as retailer_name_english,
		'Johnson & Johnson' as manufacturer_name,
		'Y' as jj_manufacturer_flag,
		ecomm.franchise AS franchise_l1,
		ecomm.need_state, 
		'na' as prod_hier_l3,
		'na' as prod_hier_l4,
		'na' AS "b1 mega-brand",
		ecomm.brand AS "b2 brand",
		'na' AS "b4 variant",
		'na' AS "b5 put-up",
		'na' AS sku,
		'na' AS product_minor_code,	
		'na' AS prod_minor,   
		'na' AS product_code,       
		null as ean,
		null as reailer_sku_code,
		'na' AS pka_product_key,
		'na' AS pka_product_key_description,
		ecomm.target_ori,
		0 AS value,	   
		0 AS usd_value,
		0 AS lcy_value,
	0 as salesweight,	   
		'na' AS from_crncy,
		'na' AS to_crncy,
		'na' AS acct_nm,
		'na' AS acct_num,
		'na' AS ciw_desc,
		'na' AS ciw_bucket,
		'na' AS csw_desc,
		ecomm.type  AS "Additional_Information",
		NULL as ppm_role  
	FROM edw_ecomm_plan ecomm
),
insert8 as(
	select * from insert8_union1
	union all
	select * from insert8_union2
	union all
	select * from insert8_union3
),
insert9 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
	'Manual NTS' AS Datasource,
	'SKU' AS Data_level,
	'NTS' AS KPI,
	'Monthly' AS Period_type,
	left(fisc_per, 4) AS fisc_year,
	LEFT(univ_per, 4) AS cal_year, -- Modified
	LTRIM(right(fisc_per, 2), '0') AS fisc_month, -- Modified
	LTRIM(RIGHT(univ_per, 2), '0') AS cal_month, -- Modified
	pos_dt AS fisc_day,
	pos_dt AS cal_day, -- Modified
	fisc_per AS fisc_yr_per,
	'Metropolitan Asia' AS cluster,
	'Korea' AS "ctry_nm", -- Modified
	'Korea Omni' AS sub_country,
	'E-Commerce' AS channel,
	'Omni' AS "sub channel", -- Modified
	'E-Commerce' AS "retail_env",
	'Direct Accounts' AS "go to model", -- Modified
	'na' AS profit_center,
	'na' AS company_code,
	'na' AS customer_code,
	sls_grp AS "parent_customer",
	sls_grp AS banner, -- Modified
	sls_grp AS "banner format", -- Modified
	NULL AS platform_name,
	str_cd || ' - ' || str_nm AS retailer_name, -- Modified
	NULL AS retailer_name_english,
	'Johnson & Johnson' AS manufacturer_name,
	'Y' AS jj_manufacturer_flag,
	prod_hier_l3 AS franchise_l1, -- Modified
	'na' AS need_state,
	'na' AS prod_hier_l3,
	'na' AS prod_hier_l4,
	'na' AS "b1 mega-brand",
	prod_hier_l4 AS "b2 brand", -- Modified
	prod_hier_l7 AS "b4 variant", -- Modified
	'na' AS "b5 put-up",
	prod_hier_l9 AS sku, -- Modified
	'na' AS product_minor_code,
	'na' AS prod_minor,
	matl_num AS product_code, -- Modified     
	ean_num AS ean, -- Modified
	vend_prod_cd AS retailer_sku_code, -- Modified
	'na' AS pka_product_key,
	'na' AS pka_product_key_description,
	0 AS target_ori,
	0 AS value,
	sls_amt * 0.74 * ex_rt AS usd_value, -- Modified
	sls_amt * 0.74 AS lcy_value,
	0 AS salesweight,
	'KRW' AS from_crncy, -- Modified
	to_crncy,
	'na' AS acct_nm,
	'na' AS acct_num,
	'na' AS ciw_desc,
	'na' AS ciw_bucket,
	'na' AS csw_desc,
	'na' AS "Additional_Information",
	NULL AS ppm_role
	FROM v_rpt_pos_offtake_daily
	WHERE ctry_nm = 'South Korea'
		AND (
			str_nm IN (
				SELECT filter_value AS "store_name"
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset = 'Korea (POS)'
					AND dataset_area = 'Store Name'
				GROUP BY 1
				)
			OR UPPER(str_nm) LIKE '%ONLINE%'
			)
		AND UPPER(sls_grp) IN (
			SELECT UPPER(filter_value) AS "customer_name"
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset = 'Korea (POS)'
				AND dataset_area = 'Customer Name'
			GROUP BY 1
			) -- Modified
		AND to_crncy = 'USD' -- Modified
		AND fisc_year - 3 < EXTRACT(YEAR FROM current_timestamp())
),
insert10_union1 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'Offtake' AS Datasource,
		'SKU' AS Data_level,
		'Offtake' AS KPI,
		'Continuous' AS Period_type,
		cal.fisc_yr AS fisc_year,
		cal.cal_yr AS cal_year,
		LTRIM("substring" (
				cal.fisc_per::CHARACTER VARYING::TEXT,
				5,
				3
				), 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
		cal.cal_mo_2 AS cal_month,
		TO_DATE((
				"substring" (
					cal.fisc_per::CHARACTER VARYING::TEXT,
					6,
					2
					) || '01'::CHARACTER VARYING::TEXT
				) || "substring" (
				cal.fisc_per::CHARACTER VARYING::TEXT,
				1,
				4
				), 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		cal.cal_day,
		cal.fisc_per AS fisc_yr_per,
		(
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Market Cluster Mapping'
				AND column_name = 'India'
				AND dataset = 'Offtake'
			) AS cluster,
		ecomm_offtake.country AS ctry_nm,
		ecomm_offtake.country AS sub_country,
		'E-Commerce' AS channel,
		NULL AS "sub channel",
		NULL AS retail_env,
		NULL AS "go to model",
		NULL AS profit_center,
		NULL AS company_code,
		NULL AS customer_code,
		ecomm_offtake.platform AS customer_name,
		NULL AS banner,
		NULL AS banner_format,
		ecomm_offtake.platform AS platform_name,
		ecomm_offtake.account_name AS retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		gcph.gcph_franchise AS franchise_l1,
		gcph.gcph_needstate,
		gcph.gcph_category,
		gcph.gcph_subcategory,
		gcph.gcph_brand,
		gcph.gcph_subbrand,
		gcph.gcph_variant,
		gcph.put_up_desc,
		(
			"first_value" (ecomm_offtake.retailer_product_name::TEXT) OVER (
				PARTITION BY ecomm_offtake.account_sku_code ORDER BY ecomm_offtake.transaction_date DESC ROWS BETWEEN UNBOUNDED PRECEDING
						AND UNBOUNDED FOLLOWING
				)
			)::CHARACTER VARYING AS retailer_product_name,
		NULL AS prod_minor_code,
		NULL AS prod_minor_name,
		gcph.matl_num AS jnj_sku_code,
		ecomm_offtake.ean,
		ecomm_offtake.account_sku_code AS retailer_sku_code,
		gcph.pka_productkey,
		gcph.pka_productdesc,
		NULL AS target_ori,
		SUM(ecomm_offtake.sales_qty) AS sales_qty,
		SUM(ecomm_offtake.sales_value * exch_rate.ex_rt::DOUBLE PRECISION) AS sales_value_usd,
		SUM(ecomm_offtake.sales_value) AS sales_value_lcy,
		0 AS salesweight,
		ecomm_offtake.transaction_currency AS from_crncy,
		exch_rate.to_crncy,
		NULL AS acct_nm,
		NULL AS acct_num,
		NULL AS ciw_desc,
		NULL AS ciw_bucket,
		NULL AS csw_desc,
		NULL AS "Additional_Information",
		NULL AS ppm_role
	FROM edw_ecommerce_offtake ecomm_offtake
	LEFT JOIN edw_calendar_dim cal ON ecomm_offtake.transaction_date::TEXT = cal.cal_day::CHARACTER VARYING::TEXT
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON ecomm_offtake.transaction_currency::TEXT = exch_rate.from_crncy::TEXT
		AND exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT
		AND cal.fisc_per = exch_rate.fisc_per
	LEFT JOIN (
		SELECT derivedtable1.ean_upc,
			derivedtable1.pka_productkey,
			derivedtable1.gcph_franchise,
			derivedtable1.gcph_brand,
			derivedtable1.gcph_subbrand,
			derivedtable1.gcph_variant,
			derivedtable1.put_up_desc,
			derivedtable1.gcph_needstate,
			derivedtable1.gcph_category,
			derivedtable1.gcph_subcategory,
			derivedtable1.gcph_segment,
			derivedtable1.gcph_subsegment,
			derivedtable1.matl_num,
			derivedtable1.pka_productdesc
		FROM (
			SELECT edw_product_key_attributes.ean_upc,
				edw_product_key_attributes.pka_productkey,
				edw_product_key_attributes.gcph_franchise,
				edw_product_key_attributes.gcph_brand,
				edw_product_key_attributes.gcph_subbrand,
				edw_product_key_attributes.gcph_variant,
				edw_product_key_attributes.put_up_desc,
				edw_product_key_attributes.gcph_needstate,
				edw_product_key_attributes.gcph_category,
				edw_product_key_attributes.gcph_subcategory,
				edw_product_key_attributes.gcph_segment,
				edw_product_key_attributes.gcph_subsegment,
				edw_product_key_attributes.matl_num,
				edw_product_key_attributes.pka_productdesc,
				edw_product_key_attributes.crt_on,
				row_number() OVER (
					PARTITION BY edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.crt_on DESC
					) AS row_num
			FROM edw_product_key_attributes
			WHERE edw_product_key_attributes.ctry_nm::TEXT = 'India'::CHARACTER VARYING::TEXT
			) derivedtable1
		WHERE derivedtable1.row_num = 1
		) gcph ON LTRIM(ecomm_offtake.ean::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(gcph.ean_upc::TEXT, 0::CHARACTER VARYING::TEXT)
	GROUP BY cal.fisc_yr,
		cal.cal_yr,
		cal.fisc_per,
		cal.cal_mo_2,
		cal.cal_day,
		ecomm_offtake.country,
		ecomm_offtake.country,
		ecomm_offtake.platform,
		ecomm_offtake.platform,
		ecomm_offtake.account_name,
		gcph.gcph_franchise,
		gcph.gcph_needstate,
		gcph.gcph_category,
		gcph.gcph_subcategory,
		gcph.gcph_brand,
		gcph.gcph_subbrand,
		gcph.gcph_variant,
		gcph.put_up_desc,
		ecomm_offtake.retailer_product_name,
		ecomm_offtake.account_sku_code,
		ecomm_offtake.transaction_date,
		gcph.matl_num,
		ecomm_offtake.ean,
		ecomm_offtake.account_sku_code,
		gcph.pka_productkey,
		gcph.pka_productdesc,
		ecomm_offtake.transaction_currency,
		exch_rate.to_crncy
),
insert10_union2 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'Offtake' AS Datasource,
		'SKU' AS Data_level,
		'Offtake' AS KPI,
		'Continuous' AS Period_type,
		cal.fisc_yr AS fisc_year,
		cal.cal_yr AS cal_year,
		LTRIM("substring" (
				cal.fisc_per::CHARACTER VARYING::TEXT,
				5,
				3
				), 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
		cal.cal_mo_2 AS cal_month,
		TO_DATE((
				"substring" (
					cal.fisc_per::CHARACTER VARYING::TEXT,
					6,
					2
					) || '01'::CHARACTER VARYING::TEXT
				) || "substring" (
				cal.fisc_per::CHARACTER VARYING::TEXT,
				1,
				4
				), 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		cal.cal_day,
		cal.fisc_per AS fisc_yr_per,
		(
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Market Cluster Mapping'
				AND column_name = 'Korea'
				AND dataset = 'Offtake'
			) AS cluster,
		ecomm_offtake_krw.country::CHARACTER VARYING AS country,
		ecomm_offtake_krw.country::CHARACTER VARYING AS sub_country,
		'E-Commerce' AS channel,
		NULL AS "sub channel",
		NULL AS retail_env,
		NULL AS "go to model",
		NULL AS profit_center,
		NULL AS company_code,
		ecomm_offtake_krw.sap_customer_code,
		CASE 
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'SSG.COM'::CHARACTER VARYING::TEXT
				THEN dt12.cust_nm
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'ECVAN'::CHARACTER VARYING::TEXT
				THEN dt12.cust_nm
			ELSE ecomm_offtake_krw.retailer_name
			END AS customer_name,
		CASE 
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'SSG.COM'::CHARACTER VARYING::TEXT
				THEN 'Emart Mall'::CHARACTER VARYING::TEXT
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = '(JU)SSG.COM'::CHARACTER VARYING::TEXT
				THEN 'Emart Mall'::CHARACTER VARYING::TEXT
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'ECVAN'::CHARACTER VARYING::TEXT
				THEN 'Emart Mall'::CHARACTER VARYING::TEXT
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'AUCTION'::CHARACTER VARYING::TEXT
				THEN 'EBAY'::CHARACTER VARYING::TEXT
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'EBAY'::CHARACTER VARYING::TEXT
				THEN 'EBAY'::CHARACTER VARYING::TEXT
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'GMARKET'::CHARACTER VARYING::TEXT
				THEN 'EBAY'::CHARACTER VARYING::TEXT
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'COUPANG'::CHARACTER VARYING::TEXT
				THEN 'COUPANG'::CHARACTER VARYING::TEXT
			ELSE UPPER(TRIM(ecomm_offtake_krw.platform::TEXT))
			END AS platform,
		NULL AS banner,
		NULL AS banner_format,
		CASE 
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'SSG.COM'::CHARACTER VARYING::TEXT
				THEN dt12.cust_nm
			WHEN UPPER(TRIM(ecomm_offtake_krw.retailer_name::TEXT)) = 'ECVAN'::CHARACTER VARYING::TEXT
				THEN dt12.cust_nm
			ELSE ecomm_offtake_krw.retailer_name
			END AS retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		gcph.gcph_franchise AS franchise_l1,
		gcph.gcph_needstate,
		gcph.gcph_category,
		gcph.gcph_subcategory,
		gcph.gcph_brand,
		gcph.gcph_subbrand,
		gcph.gcph_variant,
		gcph.put_up_desc,
		(
			"first_value" (ecomm_offtake_krw.product_title::TEXT) OVER (
				PARTITION BY ecomm_offtake_krw.retailer_sku_code ORDER BY ecomm_offtake_krw.transaction_date DESC ROWS BETWEEN UNBOUNDED PRECEDING
						AND UNBOUNDED FOLLOWING
				)
			)::CHARACTER VARYING AS retailer_product_name,
		NULL AS prod_minor_code,
		NULL AS prod_minor_name,
		gcph.matl_num AS jnj_sku_code,
		ecomm_offtake_krw.ean,
		ecomm_offtake_krw.retailer_sku_code,
		gcph.pka_productkey AS generic_product_code,
		gcph.pka_productdesc AS jnj_sku_name,
		NULL AS target_ori,
		SUM(ecomm_offtake_krw.quantity) AS sales_qty,
		SUM(ecomm_offtake_krw.sales_value * exch_rate.ex_rt::DOUBLE PRECISION) AS sales_value_usd,
		SUM(ecomm_offtake_krw.sales_value) AS sales_value_lcy,
		0 AS salesweight,
		ecomm_offtake_krw.transaction_currency AS from_crncy,
		exch_rate.to_crncy,
		NULL AS acct_nm,
		NULL AS acct_num,
		NULL AS ciw_desc,
		NULL AS ciw_bucket,
		NULL AS csw_desc,
		NULL AS "Additional_Information",
		NULL AS ppm_role
	FROM (
		SELECT INITCAP(edw_ecommerce_offtake.country::TEXT) AS country,
			edw_ecommerce_offtake.retailer_name,
			CASE 
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'SSG.COM'::CHARACTER VARYING::TEXT
					THEN '135406'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'ECVAN'::CHARACTER VARYING::TEXT
					THEN '135406'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'EBAY'::CHARACTER VARYING::TEXT
					THEN '133782'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'TREXI'::CHARACTER VARYING::TEXT
					THEN '135856'::CHARACTER VARYING
				ELSE edw_ecommerce_offtake.retailer_sku_code
				END AS retailer_sku_code,
			CASE 
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'EBAY'::CHARACTER VARYING::TEXT
					THEN '133782'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'AUCTION'::CHARACTER VARYING::TEXT
					THEN '133782'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'GMARKET'::CHARACTER VARYING::TEXT
					THEN '133782'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'TREXI'::CHARACTER VARYING::TEXT
					THEN '135856'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'SSG.COM'::CHARACTER VARYING::TEXT
					THEN '135406'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'ECVAN'::CHARACTER VARYING::TEXT
					THEN '135406'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'COUPANG'::CHARACTER VARYING::TEXT
					THEN '135124'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = '(TSP) Amazon Japan'::CHARACTER VARYING::TEXT
					THEN '132566'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'LOHACO DRUG NISHI (EC BUTSURYU)'::CHARACTER VARYING::TEXT
					THEN '620065'::CHARACTER VARYING
				WHEN UPPER(TRIM(edw_ecommerce_offtake.retailer_name::TEXT)) = 'RAKUTEN'::CHARACTER VARYING::TEXT
					THEN '133956'::CHARACTER VARYING
				ELSE edw_ecommerce_offtake.retailer_sku_code
				END AS sap_customer_code,
			edw_ecommerce_offtake.product_title,
			edw_ecommerce_offtake.ean,
			edw_ecommerce_offtake.transaction_currency,
			SUM(edw_ecommerce_offtake.sales_value) AS sales_value,
			SUM(edw_ecommerce_offtake.quantity) AS quantity,
			edw_ecommerce_offtake.sub_customer_name AS platform,
			CASE 
				WHEN to_date(edw_ecommerce_offtake.transaction_date) IS NULL
					THEN (
							"substring" (
								to_char(to_date(edw_ecommerce_offtake.load_date)),
								0,
								8
								) || '15'::CHARACTER VARYING::TEXT
							)::DATE
				ELSE to_date(edw_ecommerce_offtake.transaction_date)
				END AS transaction_date
		FROM edw_ecommerce_offtake_nta edw_ecommerce_offtake
		GROUP BY INITCAP(edw_ecommerce_offtake.country::TEXT),
			edw_ecommerce_offtake.retailer_name,
			edw_ecommerce_offtake.retailer_sku_code,
			edw_ecommerce_offtake.product_title,
			edw_ecommerce_offtake.ean,
			edw_ecommerce_offtake.transaction_currency,
			edw_ecommerce_offtake.sub_customer_name,
			CASE 
				WHEN to_date(edw_ecommerce_offtake.transaction_date) IS NULL
					THEN (
							"substring" (
								to_char(to_date(edw_ecommerce_offtake.load_date)),
								0,
								8
								) || '15'::CHARACTER VARYING::TEXT
							)::DATE
				ELSE to_date(edw_ecommerce_offtake.transaction_date)
				END
		) ecomm_offtake_krw
	LEFT JOIN edw_customer_base_dim dt12 ON LTRIM(dt12.cust_num::TEXT, '0'::CHARACTER VARYING::TEXT) = rTRIM(ecomm_offtake_krw.sap_customer_code::TEXT)
	LEFT JOIN edw_calendar_dim cal ON ecomm_offtake_krw.transaction_date::CHARACTER VARYING::DATE::CHARACTER VARYING::TEXT = cal.cal_day::CHARACTER VARYING::TEXT
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON ecomm_offtake_krw.transaction_currency::TEXT = exch_rate.from_crncy::TEXT
		AND exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT
		AND cal.fisc_per = exch_rate.fisc_per
	LEFT JOIN (
		SELECT derivedtable1.ean_upc,
			derivedtable1.pka_productkey,
			derivedtable1.gcph_franchise,
			derivedtable1.gcph_brand,
			derivedtable1.gcph_subbrand,
			derivedtable1.gcph_variant,
			derivedtable1.put_up_desc,
			derivedtable1.gcph_needstate,
			derivedtable1.gcph_category,
			derivedtable1.gcph_subcategory,
			derivedtable1.gcph_segment,
			derivedtable1.gcph_subsegment,
			derivedtable1.matl_num,
			derivedtable1.pka_productdesc
		FROM (
			SELECT edw_product_key_attributes.ean_upc,
				edw_product_key_attributes.pka_productkey,
				edw_product_key_attributes.gcph_franchise,
				edw_product_key_attributes.gcph_brand,
				edw_product_key_attributes.gcph_subbrand,
				edw_product_key_attributes.gcph_variant,
				edw_product_key_attributes.put_up_desc,
				edw_product_key_attributes.gcph_needstate,
				edw_product_key_attributes.gcph_category,
				edw_product_key_attributes.gcph_subcategory,
				edw_product_key_attributes.gcph_segment,
				edw_product_key_attributes.gcph_subsegment,
				edw_product_key_attributes.matl_num,
				edw_product_key_attributes.pka_productdesc,
				edw_product_key_attributes.crt_on,
				row_number() OVER (
					PARTITION BY edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.crt_on DESC
					) AS row_num
			FROM edw_product_key_attributes
			WHERE edw_product_key_attributes.ctry_nm::TEXT = 'Korea'::CHARACTER VARYING::TEXT
			) derivedtable1
		WHERE derivedtable1.row_num = 1
		) gcph ON LTRIM(ecomm_offtake_krw.ean::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(gcph.ean_upc::TEXT, 0::CHARACTER VARYING::TEXT)
	GROUP BY cal.fisc_yr,
		cal.cal_yr,
		cal.fisc_per,
		cal.cal_mo_2,
		cal.cal_day,
		ecomm_offtake_krw.country,
		ecomm_offtake_krw.sap_customer_code,
		ecomm_offtake_krw.retailer_name,
		gcph.gcph_franchise,
		gcph.gcph_needstate,
		gcph.gcph_category,
		gcph.gcph_subcategory,
		gcph.gcph_brand,
		gcph.gcph_subbrand,
		gcph.gcph_variant,
		gcph.put_up_desc,
		ecomm_offtake_krw.product_title,
		ecomm_offtake_krw.transaction_date,
		gcph.matl_num,
		dt12.cust_nm,
		ecomm_offtake_krw.ean,
		ecomm_offtake_krw.retailer_sku_code,
		ecomm_offtake_krw.platform,
		gcph.pka_productkey,
		gcph.pka_productdesc,
		ecomm_offtake_krw.transaction_currency,
		exch_rate.to_crncy
),
insert10_union3 as(	
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'Offtake' AS Datasource,
		'SKU' AS Data_level,
		'Offtake' AS KPI,
		'Continuous' AS Period_type,
		cal.fisc_yr AS fisc_year,
		cal.cal_yr AS cal_year,
		LTRIM("substring" (
				cal.fisc_per::CHARACTER VARYING::TEXT,
				5,
				3
				), 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
		cal.cal_mo_2 AS cal_month,
		TO_DATE((
				"substring" (
					cal.fisc_per::CHARACTER VARYING::TEXT,
					6,
					2
					) || '01'::CHARACTER VARYING::TEXT
				) || "substring" (
				cal.fisc_per::CHARACTER VARYING::TEXT,
				1,
				4
				), 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		cal.cal_day,
		cal.fisc_per AS fisc_yr_per,
		(
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Market Cluster Mapping'
				AND column_name = 'Korea'
				AND dataset = 'Offtake'
			) AS cluster,
		ims_txn.country,
		ims_txn.country AS sub_country,
		'E-Commerce' AS Channel,
		NULL AS "sub channel",
		NULL AS retail_env,
		NULL AS "go to model",
		NULL AS profit_center,
		NULL AS company_code,
		ims_txn.retailer_sku_code AS sap_customer_code,
		ims_txn.retailer_name AS customer_name,
		NULL AS banner,
		NULL AS banner_format,
		UPPER(ims_txn.platform::TEXT)::CHARACTER VARYING AS platform,
		ims_txn.retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		gcph.gcph_franchise AS franchise_l1,
		gcph.gcph_needstate,
		gcph.gcph_category,
		gcph.gcph_subcategory,
		gcph.gcph_brand,
		gcph.gcph_subbrand,
		gcph.gcph_variant,
		gcph.put_up_desc,
		ims_txn.product_title AS retailer_product_name,
		NULL AS product_minor_code,
		NULL AS product_minor,
		gcph.matl_num AS jnj_sku_code,
		ims_txn.ean,
		ims_txn.retailer_sku_code,
		gcph.pka_productkey AS generic_product_code,
		gcph.pka_productdesc AS jnj_sku_name,
		NULL AS target_ori,
		SUM(ims_txn.quantity) AS sales_qty,
		SUM(ims_txn.sales_value::DOUBLE PRECISION * exch_rate.ex_rt::DOUBLE PRECISION) AS sales_value_usd,
		SUM(ims_txn.sales_value) AS sales_value_lcy,
		0 AS salesweight,
		ims_txn.transaction_currency AS from_crncy,
		exch_rate.to_crncy,
		NULL AS acct_nm,
		NULL AS acct_num,
		NULL AS ciw_desc,
		NULL AS ciw_bucket,
		NULL AS csw_desc,
		NULL AS "Additional_Information",
		NULL AS ppm_role
	FROM (
		SELECT 'Korea'::CHARACTER VARYING AS country,
			edw_ims_fact.dstr_nm AS retailer_name,
			edw_ims_fact.dstr_cd AS retailer_sku_code,
			edw_ims_fact.prod_nm AS product_title,
			edw_ims_fact.ean_num AS ean,
			edw_ims_fact.crncy_cd AS transaction_currency,
			SUM(edw_ims_fact.sls_amt) AS sales_value,
			SUM(edw_ims_fact.sls_qty) AS quantity,
			edw_ims_fact.ims_txn_dt AS transaction_date,
			edw_ims_fact.cust_nm AS platform
		FROM edw_ims_fact
		WHERE edw_ims_fact.ctry_cd::TEXT = 'KR'::CHARACTER VARYING::TEXT
			AND (
				edw_ims_fact.dstr_cd::TEXT = '129057'::CHARACTER VARYING::TEXT
				OR edw_ims_fact.dstr_cd::TEXT = '135139'::CHARACTER VARYING::TEXT
				)
		GROUP BY INITCAP(edw_ims_fact.ctry_cd::TEXT),
			edw_ims_fact.dstr_nm,
			edw_ims_fact.dstr_cd,
			edw_ims_fact.prod_nm,
			edw_ims_fact.ean_num,
			edw_ims_fact.crncy_cd,
			edw_ims_fact.ims_txn_dt,
			edw_ims_fact.cust_nm
		) ims_txn
	LEFT JOIN edw_calendar_dim cal ON ims_txn.transaction_date::CHARACTER VARYING::DATE::CHARACTER VARYING::TEXT = cal.cal_day::CHARACTER VARYING::TEXT
	LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON ims_txn.transaction_currency::TEXT = exch_rate.from_crncy::TEXT
		AND exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT
		AND cal.fisc_per = exch_rate.fisc_per
	LEFT JOIN (
		SELECT derivedtable1.ean_upc,
			derivedtable1.pka_productkey,
			derivedtable1.gcph_franchise,
			derivedtable1.gcph_brand,
			derivedtable1.gcph_subbrand,
			derivedtable1.gcph_variant,
			derivedtable1.put_up_desc,
			derivedtable1.gcph_needstate,
			derivedtable1.gcph_category,
			derivedtable1.gcph_subcategory,
			derivedtable1.gcph_segment,
			derivedtable1.gcph_subsegment,
			derivedtable1.matl_num,
			derivedtable1.pka_productdesc
		FROM (
			SELECT edw_product_key_attributes.ean_upc,
				edw_product_key_attributes.pka_productkey,
				edw_product_key_attributes.gcph_franchise,
				edw_product_key_attributes.gcph_brand,
				edw_product_key_attributes.gcph_subbrand,
				edw_product_key_attributes.gcph_variant,
				edw_product_key_attributes.put_up_desc,
				edw_product_key_attributes.gcph_needstate,
				edw_product_key_attributes.gcph_category,
				edw_product_key_attributes.gcph_subcategory,
				edw_product_key_attributes.gcph_segment,
				edw_product_key_attributes.gcph_subsegment,
				edw_product_key_attributes.matl_num,
				edw_product_key_attributes.pka_productdesc,
				edw_product_key_attributes.crt_on,
				row_number() OVER (
					PARTITION BY edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.crt_on DESC
					) AS row_num
			FROM edw_product_key_attributes
			WHERE edw_product_key_attributes.ctry_nm::TEXT = 'Korea'::CHARACTER VARYING::TEXT
			) derivedtable1
		WHERE derivedtable1.row_num = 1
		) gcph ON LTRIM(ims_txn.ean::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(gcph.ean_upc::TEXT, 0::CHARACTER VARYING::TEXT)
	GROUP BY cal.fisc_yr,
		cal.cal_yr,
		cal.fisc_per,
		cal.cal_mo_2,
		cal.cal_day,
		ims_txn.country,
		ims_txn.country,
		ims_txn.retailer_sku_code,
		ims_txn.retailer_name,
		platform,
		ims_txn.retailer_name,
		gcph.gcph_franchise,
		gcph.gcph_needstate,
		gcph.gcph_category,
		gcph.gcph_subcategory,
		gcph.gcph_brand,
		gcph.gcph_subbrand,
		gcph.gcph_variant,
		gcph.put_up_desc,
		ims_txn.product_title,
		gcph.matl_num,
		ims_txn.ean,
		ims_txn.retailer_sku_code,
		gcph.pka_productkey,
		gcph.pka_productdesc,
		ims_txn.transaction_currency,
		exch_rate.to_crncy
),
insert10_union4 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'Offtake' AS Datasource,
		'SKU' AS Data_level,
		'Offtake' AS KPI,
		'Continuous' AS Period_type,
		ph_ecomm_offtake.fisc_yr AS fisc_year,
		ph_ecomm_offtake.cal_yr AS cal_year,
		LTRIM("substring" (
				ph_ecomm_offtake.fisc_per::CHARACTER VARYING::TEXT,
				5,
				3
				), 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
		ph_ecomm_offtake.cal_mo_2 AS cal_month,
		TO_DATE((
				"substring" (
					ph_ecomm_offtake.fisc_per::CHARACTER VARYING::TEXT,
					6,
					2
					) || '01'::CHARACTER VARYING::TEXT
				) || "substring" (
				ph_ecomm_offtake.fisc_per::CHARACTER VARYING::TEXT,
				1,
				4
				), 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		ph_ecomm_offtake.cal_day,
		ph_ecomm_offtake.fisc_per AS fisc_yr_per,
		(
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Market Cluster Mapping'
				AND column_name = 'Philippines'
				AND dataset = 'Offtake'
			) AS cluster,
		market,
		ph_ecomm_offtake.market AS sub_market,
		'E-Commerce' AS channel,
		NULL AS "sub channel",
		NULL AS retail_env,
		NULL AS "go to model",
		NULL AS profit_center,
		NULL AS company_code,
		NULL AS sap_customer_code,
		ph_ecomm_offtake.platform_name AS sap_customer_name,
		NULL AS banner,
		NULL AS banner_format,
		platform_name,
		retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		ph_ecomm_offtake.gcph_franchise AS prod_hier_l1,
		gcph_needstate,
		gcph_category,
		gcph_subcategory,
		gcph_brand,
		gcph_subbrand,
		pka_variantdesc,
		put_up_description,
		(
			"first_value" (ph_ecomm_offtake.product_desc::TEXT) OVER (
				PARTITION BY ph_ecomm_offtake.retailer_product_code ORDER BY ph_ecomm_offtake.transaction_date DESC ROWS BETWEEN UNBOUNDED PRECEDING
						AND UNBOUNDED FOLLOWING
				)
			)::CHARACTER VARYING AS retailer_product_name,
		NULL AS product_minor_code,
		NULL AS product_minor_name,
		matl_num AS material_number,
		ph_ecomm_offtake.ean_upc,
		ph_ecomm_offtake.retailer_product_code AS retailer_sku_code,
		ph_ecomm_offtake.pka_product_name AS product_key,
		ph_ecomm_offtake.pka_product_name AS product_key_description,
		NULL AS target_ori,
		SUM(ph_ecomm_offtake.sales_quantity) AS sales_qty,
		SUM(sales_value_usd) AS sales_value_usd,
		SUM(ph_ecomm_offtake.sales_value_lcy) AS sales_value_lcy,
		0 AS salesweight,
		from_crncy,
		to_crncy,
		NULL AS acct_nm,
		NULL AS acct_num,
		NULL AS ciw_desc,
		NULL AS ciw_bucket,
		NULL AS csw_desc,
		NULL AS "Additional_Information",
		NULL AS ppm_role
	FROM edw_ph_ecommerce_offtake ph_ecomm_offtake
	WHERE upper(ph_ecomm_offtake.delivery_status) <> 'CANCELLED'
		AND upper(ph_ecomm_offtake.delivery_status) <> 'REJECTED'
	GROUP BY ph_ecomm_offtake.fisc_yr,
		ph_ecomm_offtake.cal_yr,
		ph_ecomm_offtake.fisc_per,
		ph_ecomm_offtake.cal_mo_2,
		ph_ecomm_offtake.cal_day,
		ph_ecomm_offtake.market,
		ph_ecomm_offtake.platform_name,
		ph_ecomm_offtake.retailer_name,
		ph_ecomm_offtake.gcph_franchise,
		ph_ecomm_offtake.gcph_needstate,
		ph_ecomm_offtake.gcph_category,
		ph_ecomm_offtake.gcph_subcategory,
		ph_ecomm_offtake.gcph_brand,
		ph_ecomm_offtake.gcph_subbrand,
		ph_ecomm_offtake.pka_variantdesc,
		ph_ecomm_offtake.put_up_description,
		ph_ecomm_offtake.product_desc,
		ph_ecomm_offtake.retailer_product_code,
		ph_ecomm_offtake.transaction_date,
		ph_ecomm_offtake.matl_num,
		ph_ecomm_offtake.ean_upc,
		ph_ecomm_offtake.pka_product_name,
		ph_ecomm_offtake.from_crncy,
		ph_ecomm_offtake.to_crncy 
),
insert10 as(
	select * from insert10_union1
	union all
	select * from insert10_union2
	union all
	select * from insert10_union3
	union all
	select * from insert10_union4
),
insert11 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'SG_GTS' AS Datasource,
		'BRAND' AS Data_level,
		'GTS' AS KPI,
		'Monthly' AS Period_type,
		derived_table1.year::CHARACTER VARYING AS fisc_year,
		NULL AS cal_year,
		derived_table1.mnth_no::CHARACTER VARYING AS fisc_month,
		NULL AS cal_month,
		TO_DATE((LPAD(derived_table1.mnth_no::CHARACTER VARYING::TEXT, 2, '0'::CHARACTER VARYING::TEXT) || '01'::CHARACTER VARYING::TEXT) || derived_table1.year::CHARACTER VARYING::TEXT, 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		NULL AS cal_day,
		(derived_table1.year::CHARACTER VARYING::TEXT || LPAD(derived_table1.mnth_no::CHARACTER VARYING::TEXT, 2, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING::TEXT)::INTEGER AS fisc_yr_per,
		'Metropolitan Asia' AS "cluster",
		'Singapore' AS ctry_nm,
		'Singapore' AS sub_country,
		'na' AS channel,
		'na' AS "sub channel",
		'E-Commerce' AS retail_env,
		'Indirect Accounts' AS "go to model",
		'na' AS profit_center,
		'na' AS company_code,
		'na' AS customer_code,
		derived_table1.sg_banner AS "parent customer",
		'na' AS banner,
		'na' AS "banner format",
		NULL AS platform_name,
		NULL AS retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		derived_table1.gph_reg_frnchse_grp AS prod_hier_l1,
		'na' AS prod_hier_l2,
		'na' AS prod_hier_l3,
		'na' AS prod_hier_l4,
		'na' AS prod_hier_l5,
		derived_table1.sg_brand AS prod_hier_l6,
		'na' AS prod_hier_l7,
		'na' AS prod_hier_l8,
		'na' AS prod_hier_l9,
		'na' AS product_minor_code,
		'na' AS product_minor_name,
		'na' AS material_name,
		NULL AS ean,
		NULL AS reailer_sku_code,
		'na' AS pka_product_key,
		'na' AS pka_product_key_description,
		0 AS target_ori,
		0 AS value,
		SUM(derived_table1.gts)::NUMERIC(18, 0) AS gts_usd,
		0 AS gts_lcy,
		0 AS salesweight,
		'na' AS from_crncy,
		'na' AS to_crncy,
		'na' AS acct_nm,
		'na' AS acct_num,
		'na' AS ciw_desc,
		'na' AS ciw_bucket,
		'na' AS csw_desc,
		'na' AS "Additional_Information",
		NULL AS ppm_role
	FROM (
		SELECT edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner,
			SUM(edw_sg_sellin_analysis.base_value) AS gts,
			0 AS tp
		FROM edw_sg_sellin_analysis
		WHERE edw_sg_sellin_analysis.retail_env::TEXT = 'E-Commerce'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.year >= EXTRACT(YEAR FROM current_timestamp()) - 2
			AND edw_sg_sellin_analysis.measure_bucket::TEXT = 'GTS'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.cust_l1::TEXT = 'Zuellig'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.currency::TEXT = 'USD'::CHARACTER VARYING::TEXT
		GROUP BY edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner
		
		UNION ALL
		
		SELECT edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner,
			0 AS gts,
			SUM(edw_sg_sellin_analysis.base_value) AS tp
		FROM edw_sg_sellin_analysis
		WHERE edw_sg_sellin_analysis.retail_env::TEXT = 'E-Commerce'::CHARACTER VARYING::TEXT
			AND (
				edw_sg_sellin_analysis.year = 2019
				OR edw_sg_sellin_analysis.year = 2020
				OR edw_sg_sellin_analysis.year = 2021
				)
			AND (
				edw_sg_sellin_analysis.measure_bucket::TEXT = 'Return'::CHARACTER VARYING::TEXT
				OR edw_sg_sellin_analysis.measure_bucket::TEXT = 'TT'::CHARACTER VARYING::TEXT
				OR edw_sg_sellin_analysis.measure_bucket::TEXT = 'TP On-invoice'::CHARACTER VARYING::TEXT
				OR edw_sg_sellin_analysis.measure_bucket::TEXT = 'TP Off-invoice (Accrual)'::CHARACTER VARYING::TEXT
				)
			AND edw_sg_sellin_analysis.cust_l1::TEXT = 'Zuellig'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.currency::TEXT = 'USD'::CHARACTER VARYING::TEXT
		GROUP BY edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner
		) derived_table1
	GROUP BY derived_table1.year,
		derived_table1.mnth_no,
		derived_table1.gph_reg_frnchse_grp,
		derived_table1.sg_brand,
		derived_table1.sg_banner
),
insert12 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'SG_NTS' AS Datasource,
		'BRAND' AS Data_level,
		'NTS' AS KPI,
		'Monthly' AS Period_type,
		derived_table1.year::CHARACTER VARYING AS fisc_year,
		NULL AS cal_year,
		derived_table1.mnth_no::CHARACTER VARYING AS fisc_month,
		NULL AS cal_month,
		TO_DATE((LPAD(derived_table1.mnth_no::CHARACTER VARYING::TEXT, 2, '0'::CHARACTER VARYING::TEXT) || '01'::CHARACTER VARYING::TEXT) || derived_table1.year::CHARACTER VARYING::TEXT, 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
		NULL AS cal_day,
		(derived_table1.year::CHARACTER VARYING::TEXT || LPAD(derived_table1.mnth_no::CHARACTER VARYING::TEXT, 2, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING::TEXT)::INTEGER AS fisc_yr_per,
		'Metropolitan Asia' AS "cluster",
		'Singapore' AS ctry_nm,
		'Singapore' AS sub_country,
		'na' AS channel,
		'na' AS "sub channel",
		'E-Commerce' AS retail_env,
		'Indirect Accounts' AS "go to model",
		'na' AS profit_center,
		'na' AS company_code,
		'na' AS customer_code,
		derived_table1.sg_banner AS "parent customer",
		'na' AS banner,
		'na' AS "banner format",
		NULL AS platform_name,
		NULL AS retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		derived_table1.gph_reg_frnchse_grp AS prod_hier_l1,
		'na' AS prod_hier_l2,
		'na' AS prod_hier_l3,
		'na' AS prod_hier_l4,
		'na' AS prod_hier_l5,
		derived_table1.sg_brand AS prod_hier_l6,
		'na' AS prod_hier_l7,
		'na' AS prod_hier_l8,
		'na' AS prod_hier_l9,
		'na' AS product_minor_code,
		'na' AS product_minor_name,
		'na' AS material_name,
		NULL AS ean,
		NULL AS reailer_sku_code,
		'na' AS pka_product_key,
		'na' AS pka_product_key_description,
		0 AS target_ori,
		0 AS value,
		SUM(derived_table1.gts - derived_table1.tp)::NUMERIC(18, 0) AS nts_usd,
		0 AS nts_lcy,
		0 AS salesweight,
		'na' AS from_crncy,
		'na' AS to_crncy,
		'na' AS acct_nm,
		'na' AS acct_num,
		'na' AS ciw_desc,
		'na' AS ciw_bucket,
		'na' AS csw_desc,
		'na' AS "Additional_Information",
		NULL AS ppm_role
	FROM (
		SELECT edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner,
			SUM(edw_sg_sellin_analysis.base_value) AS gts,
			0 AS tp
		FROM edw_sg_sellin_analysis
		WHERE edw_sg_sellin_analysis.retail_env::TEXT = 'E-Commerce'::CHARACTER VARYING::TEXT
			AND (
				edw_sg_sellin_analysis.year = 2019
				OR edw_sg_sellin_analysis.year = 2020
				OR edw_sg_sellin_analysis.year = 2021
				)
			AND edw_sg_sellin_analysis.measure_bucket::TEXT = 'GTS'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.cust_l1::TEXT = 'Zuellig'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.currency::TEXT = 'USD'::CHARACTER VARYING::TEXT
		GROUP BY edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner
		
		UNION ALL
		
		SELECT edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner,
			0 AS gts,
			SUM(edw_sg_sellin_analysis.base_value) AS tp
		FROM edw_sg_sellin_analysis
		WHERE edw_sg_sellin_analysis.retail_env::TEXT = 'E-Commerce'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.year >= EXTRACT(YEAR FROM current_timestamp()) - 2
			AND (
				edw_sg_sellin_analysis.measure_bucket::TEXT = 'Return'::CHARACTER VARYING::TEXT
				OR edw_sg_sellin_analysis.measure_bucket::TEXT = 'TT'::CHARACTER VARYING::TEXT
				OR edw_sg_sellin_analysis.measure_bucket::TEXT = 'TP On-invoice'::CHARACTER VARYING::TEXT
				OR edw_sg_sellin_analysis.measure_bucket::TEXT = 'TP Off-invoice (Accrual)'::CHARACTER VARYING::TEXT
				)
			AND edw_sg_sellin_analysis.cust_l1::TEXT = 'Zuellig'::CHARACTER VARYING::TEXT
			AND edw_sg_sellin_analysis.currency::TEXT = 'USD'::CHARACTER VARYING::TEXT
		GROUP BY edw_sg_sellin_analysis.year,
			edw_sg_sellin_analysis.mnth_no,
			edw_sg_sellin_analysis.gph_reg_frnchse_grp,
			edw_sg_sellin_analysis.sg_brand,
			edw_sg_sellin_analysis.sg_banner
		) derived_table1
	GROUP BY derived_table1.year,
		derived_table1.mnth_no,
		derived_table1.gph_reg_frnchse_grp,
		derived_table1.sg_brand,
		derived_table1.sg_banner
),
insert13 as(
	SELECT 'Act'::CHARACTER VARYING AS data_type,
		'CIW' AS Datasource,
		'Aggregated' AS Data_level,
		'CIW' AS KPI,
		'Monthly' AS Period_type,
		fact.fisc_yr::CHARACTER VARYING AS fisc_year,
		NULL AS cal_year,
		fact.fisc_month,
		NULL AS cal_month,
		fact.fisc_day,
		NULL::DATE AS cal_day,
		fact.fisc_yr_per,
		CASE 
			WHEN (
					ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134559'::CHARACTER VARYING::TEXT
					OR ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134106'::CHARACTER VARYING::TEXT
					OR ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134258'::CHARACTER VARYING::TEXT
					OR ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134855'::CHARACTER VARYING::TEXT
					)
				AND ltrim(fact.acct_num::TEXT, 0::CHARACTER VARYING::TEXT) <> '403185'::CHARACTER VARYING::TEXT
				AND mat.mega_brnd_desc::TEXT <> 'Vogue Int/l'::CHARACTER VARYING::TEXT
				AND fact.fisc_yr = 2018
				THEN 'China'::CHARACTER VARYING
			WHEN company."cluster" = 'China Personal Care'
				THEN 'China'
			ELSE company."cluster"
			END AS "cluster",
		CASE 
			WHEN (
					LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134559'::CHARACTER VARYING::TEXT
					OR LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134106'::CHARACTER VARYING::TEXT
					OR LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134258'::CHARACTER VARYING::TEXT
					OR LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134855'::CHARACTER VARYING::TEXT
					)
				AND LTRIM(fact.acct_num::TEXT, 0::CHARACTER VARYING::TEXT) <> '403185'::CHARACTER VARYING::TEXT
				AND mat.mega_brnd_desc::TEXT <> 'Vogue Int/l'::CHARACTER VARYING::TEXT
				AND fact.fisc_yr = 2018
				THEN 'China Personal Care'::CHARACTER VARYING
			WHEN company.ctry_group::TEXT = 'China'::CHARACTER VARYING::TEXT
				THEN 'China Personal Care'::CHARACTER VARYING
			WHEN company.ctry_group IN ('Australia', 'New Zealand')
				THEN 'Pacific'
			ELSE company.ctry_group
			END AS ctry_nm,
		CASE 
			WHEN (
					LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134559'::CHARACTER VARYING::TEXT
					OR LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134106'::CHARACTER VARYING::TEXT
					OR LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134258'::CHARACTER VARYING::TEXT
					OR LTRIM(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134855'::CHARACTER VARYING::TEXT
					)
				AND LTRIM(fact.acct_num::TEXT, 0::CHARACTER VARYING::TEXT) <> '403185'::CHARACTER VARYING::TEXT
				AND mat.mega_brnd_desc::TEXT <> 'Vogue Int/l'::CHARACTER VARYING::TEXT
				AND fact.fisc_yr = 2018
				THEN 'China Personal Care'::CHARACTER VARYING
			WHEN company.ctry_group::TEXT = 'China'::CHARACTER VARYING::TEXT
				THEN 'China Personal Care'::CHARACTER VARYING
			WHEN company.ctry_group IN ('Australia', 'New Zealand')
				THEN 'Pacific'
			WHEN company.ctry_group = 'APSC Regional'
				THEN 'China Personal Care'
			ELSE company.ctry_group
			END AS sub_country,
		cus_sales_extn.channel,
		cus_sales_extn."sub channel",
		cus_sales_extn.retail_env,
		cus_sales_extn."go to model",
		fact.prft_ctr::CHARACTER VARYING AS profit_center,
		company.co_cd AS company_code,
		fact.cust_num AS customer_code,
		cus_sales_extn."parent customer",
		cus_sales_extn.banner,
		cus_sales_extn."banner format",
		NULL AS platform_name,
		NULL AS retailer_name,
		NULL AS retailer_name_english,
		'Johnson & Johnson' AS manufacturer_name,
		'Y' AS jj_manufacturer_flag,
		fact.franchise_l1,
		fact.need_state,
		'na' AS prod_hier_l3,
		'na' AS prod_hier_l4,
		mat.mega_brnd_desc AS "b1 mega-brand",
		mat.brnd_desc AS "b2 brand",
		mat.varnt_desc AS "b4 variant",
		mat.put_up_desc AS "b5 put-up",
		mat.matl_desc AS sku,
		mat.prodh5 AS product_minor_code,
		fact.prod_minor,
		LTRIM(fact.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS product_code,
		NULL AS ean,
		NULL AS reailer_sku_code,
		mat.pka_product_key,
		mat.pka_product_key_description,
		0 AS target_ori,
		SUM(fact.qty) AS value,
		SUM(fact.amt_usd) AS usd_value,
		SUM(fact.amt_lcy) AS lcy_value,
		0 AS salesweight,
		fact.from_crncy,
		fact.to_crncy,
		ciw.acct_nm,
		LTRIM(ciw.acct_num::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS acct_num,
		ciw.ciw_desc,
		ciw.ciw_bucket,
		csw.csw_desc,
		'na' AS "Additional_Information",
		NULL AS ppm_role
	FROM (
		SELECT copa.fisc_yr,
			copa.fisc_yr_per,
			LTRIM("substring" (
					copa.fisc_yr_per::CHARACTER VARYING::TEXT,
					5,
					3
					), '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_month,
			TO_DATE((
					"substring" (
						copa.fisc_yr_per::CHARACTER VARYING::TEXT,
						6,
						8
						) || '01'::CHARACTER VARYING::TEXT
					) || "substring" (
					copa.fisc_yr_per::CHARACTER VARYING::TEXT,
					1,
					4
					), 'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
			copa.acct_num,
			copa.obj_crncy_co_obj,
			copa.matl_num,
			copa.co_cd,
			copa.sls_org,
			copa.dstr_chnl,
			copa.div,
			copa.cust_num,
			exch_rate.from_crncy,
			exch_rate.to_crncy,
			copa.acct_hier_shrt_desc,
			SUM(copa.qty) AS qty,
			SUM(copa.amt_obj_crncy) AS amt_lcy,
			SUM(copa.amt_obj_crncy * exch_rate.ex_rt) AS amt_usd,
			prod_map.prod_minor,
			prod_map.franchise_l1,
			prod_map.franchise_l2,
			prod_map.franchise_l3,
			prod_map.franchise_l4,
			LTRIM(copa.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) AS prft_ctr,
			prod_map.need_state
		FROM edw_copa_trans_fact copa
		LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON copa.obj_crncy_co_obj::TEXT = exch_rate.from_crncy::TEXT
			AND exch_rate.to_crncy = (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset_area = 'From Currency'
					AND column_name = 'to_crncy'
					AND dataset = 'CIW (SAP)'
				)
			AND copa.fisc_yr_per = exch_rate.fisc_per
		LEFT JOIN edw_profit_center_franchise_mapping prod_map ON LTRIM(copa.prft_ctr::TEXT, '0'::CHARACTER VARYING::TEXT) = LTRIM(prod_map.profit_center::TEXT, '0'::CHARACTER VARYING::TEXT)
		WHERE copa.acct_hier_shrt_desc = (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset_area = 'Account Hierarchy Inclusion'
					AND column_name = 'acct_hier_shrt_desc'
					AND dataset = 'CIW (SAP)'
				)
			AND copa.fisc_yr_per >= (
				SELECT (
						SELECT fisc_yr
						FROM EDW_CALENDAR_DIM
						WHERE cal_day = to_date(convert_timezone('UTC',current_timestamp())) - 365
						) - filter_value::float || '001' AS filter_value
				FROM ITG_MDS_AP_ECOM_ONEVIEW_CONFIG
				WHERE column_name = 'fisc_yr_per'
					AND dataset = 'CIW (SAP)'
				)
			AND ltrim(copa.acct_num, '0') NOT IN (
				SELECT filter_value
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset_area = 'CIW Account Code Exclusion'
					AND column_name = 'acct_num'
					AND dataset = 'CIW (SAP)'
				)
		GROUP BY copa.fisc_yr,
			copa.fisc_yr_per,
			copa.obj_crncy_co_obj,
			copa.acct_num,
			copa.matl_num,
			copa.co_cd,
			copa.sls_org,
			copa.dstr_chnl,
			copa.div,
			copa.cust_num,
			copa.acct_hier_shrt_desc,
			exch_rate.from_crncy,
			exch_rate.to_crncy,
			prod_map.prod_minor,
			prod_map.franchise_l1,
			prod_map.franchise_l2,
			prod_map.franchise_l3,
			prod_map.franchise_l4,
			copa.prft_ctr,
			prod_map.need_state
		) fact
	LEFT JOIN edw_acct_ciw_hier ciw ON LTRIM(fact.acct_num::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(ciw.acct_num::TEXT, 0::CHARACTER VARYING::TEXT)
		AND fact.acct_hier_shrt_desc::TEXT = ciw.measure_code::TEXT
	LEFT JOIN (
		SELECT a.acct_num,
			b.csw_acct_hier_name AS csw_desc
		FROM (
			SELECT edw_account_ciw_dim.acct_num,
				edw_account_ciw_dim.bravo_acct_l3,
				edw_account_ciw_dim.bravo_acct_l4
			FROM edw_account_ciw_dim
			WHERE edw_account_ciw_dim.chrt_acct = (
					SELECT filter_value
					FROM itg_mds_ap_ecom_oneview_config
					WHERE dataset_area = 'Account Type'
						AND column_name = 'chrt_acct'
						AND dataset = 'CIW (SAP)'
					)
				AND ltrim(edw_account_ciw_dim.acct_num, '0') NOT IN (
					SELECT filter_value
					FROM itg_mds_ap_ecom_oneview_config
					WHERE dataset_area = 'CIW Account Code Exclusion'
						AND column_name = 'acct_num'
						AND dataset = 'CIW (SAP)'
					)
				AND edw_account_ciw_dim.bravo_acct_l2::TEXT = 'JJPLAC510001'::CHARACTER VARYING::TEXT
			) a
		LEFT JOIN (
			SELECT 'JJPLAC512200'::CHARACTER VARYING AS csw_acct_hier_no,
				'Sales Return'::CHARACTER VARYING AS csw_acct_hier_name
			
			UNION ALL
			
			SELECT 'JJPLAC512001'::CHARACTER VARYING AS csw_acct_hier_no,
				'Sales Discount & Reserve'::CHARACTER VARYING AS csw_acct_hier_name
			
			UNION ALL
			
			SELECT 'JJPLAC513001'::CHARACTER VARYING AS csw_acct_hier_no,
				'Sales Incentive'::CHARACTER VARYING AS csw_acct_hier_name
			
			UNION ALL
			
			SELECT 'JJPLAC514001'::CHARACTER VARYING AS csw_acct_hier_no,
				'Promo & Trade related'::CHARACTER VARYING AS csw_acct_hier_name
			
			UNION ALL
			
			SELECT 'JJPLAC511000'::CHARACTER VARYING AS csw_acct_hier_no,
				'Gross Trade Prod Sales'::CHARACTER VARYING AS csw_acct_hier_name
			) b ON CASE 
				WHEN a.bravo_acct_l4::TEXT = 'JJPLAC512200'::CHARACTER VARYING::TEXT
					THEN b.csw_acct_hier_no::TEXT = a.bravo_acct_l4::TEXT
				ELSE b.csw_acct_hier_no::TEXT = a.bravo_acct_l3::TEXT
				END
		) csw ON LTRIM(fact.acct_num::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(csw.acct_num::TEXT, 0::CHARACTER VARYING::TEXT)
	LEFT JOIN edw_material_dim mat ON fact.matl_num::TEXT = mat.matl_num::TEXT
	JOIN edw_company_dim company ON fact.co_cd::TEXT = company.co_cd::TEXT
	LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON fact.sls_org::TEXT = cus_sales_extn.sls_org::TEXT
		AND fact.dstr_chnl::TEXT = cus_sales_extn.dstr_chnl::TEXT
		AND fact.div::TEXT = cus_sales_extn.div::TEXT
		AND fact.cust_num::TEXT = cus_sales_extn.cust_num::TEXT
	LEFT JOIN edw_gch_producthierarchy gph ON fact.matl_num::TEXT = gph.materialnumber::TEXT
	LEFT JOIN edw_gch_customerhierarchy gch ON fact.cust_num::TEXT = gch.customer::TEXT
	WHERE cus_sales_extn.retail_env = (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Channel Type'
				AND column_name = 'channel'
				AND dataset = 'CIW (SAP)'
			)
		AND company.ctry_group NOT IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Country Exclusion'
				AND column_name = 'ctry_group'
				AND dataset = 'CIW (SAP)'
			)
		AND company."cluster" NOT IN (
			SELECT filter_value
			FROM itg_mds_ap_ecom_oneview_config
			WHERE dataset_area = 'Cluster Exclusion'
				AND column_name = 'cluster'
				AND dataset = 'CIW (SAP)'
			)
	GROUP BY fact.fisc_yr,
		fact.fisc_month,
		fact.fisc_day,
		fact.fisc_yr_per,
		CASE 
			WHEN (
					ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134559'::CHARACTER VARYING::TEXT
					OR ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134106'::CHARACTER VARYING::TEXT
					OR ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134258'::CHARACTER VARYING::TEXT
					OR ltrim(fact.cust_num::TEXT, 0::CHARACTER VARYING::TEXT) = '134855'::CHARACTER VARYING::TEXT
					)
				AND ltrim(fact.acct_num::TEXT, 0::CHARACTER VARYING::TEXT) <> '403185'::CHARACTER VARYING::TEXT
				AND mat.mega_brnd_desc::TEXT <> $$Vogue Int\' l $$::CHARACTER VARYING::TEXT AND fact.fisc_yr = 2018 THEN ' China Selfcare '::CHARACTER VARYING
			-- WHEN company."cluster" = ' China Personal Care ' THEN ' China '
				ELSE company."cluster" 
		END  ,
		company."cluster", 
		CASE
			WHEN (LTRIM(fact.cust_num::TEXT,0::CHARACTER VARYING::TEXT) = ' 134559 '::CHARACTER VARYING::TEXT OR LTRIM(fact.cust_num::TEXT,0::CHARACTER VARYING::TEXT) = ' 134106 '::CHARACTER VARYING::TEXT OR LTRIM(fact.cust_num::TEXT,0::CHARACTER VARYING::TEXT) = ' 134258 '::CHARACTER VARYING::TEXT OR LTRIM(fact.cust_num::TEXT,0::CHARACTER VARYING::TEXT) = ' 134855 '::CHARACTER VARYING::TEXT) AND LTRIM(fact.acct_num::TEXT,0::CHARACTER VARYING::TEXT) <> ' 403185 '::CHARACTER VARYING::TEXT AND mat.mega_brnd_desc::TEXT <> $$Vogue Int\' l $$::CHARACTER VARYING::TEXT
				AND fact.fisc_yr = 2018
				THEN 'China Selfcare'::CHARACTER VARYING
			ELSE company.ctry_group
			END,
		company.ctry_group,
		cus_sales_extn.channel,
		cus_sales_extn."sub channel",
		cus_sales_extn.retail_env,
		cus_sales_extn."go to model",
		fact.prft_ctr,
		company.co_cd,
		fact.cust_num,
		cus_sales_extn."parent customer",
		cus_sales_extn.banner,
		cus_sales_extn."banner format",
		fact.franchise_l1,
		fact.need_state,
		mat.mega_brnd_desc,
		mat.brnd_desc,
		mat.varnt_desc,
		mat.put_up_desc,
		mat.matl_desc,
		mat.prodh5,
		fact.prod_minor,
		LTRIM(fact.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING,
		mat.pka_product_key,
		mat.pka_product_key_description,
		fact.from_crncy,
		fact.to_crncy,
		ciw.acct_nm,
		fact.acct_num,
		LTRIM(ciw.acct_num::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING,
		ciw.ciw_desc,
		ciw.ciw_bucket,
		csw.csw_desc
),

insert14 as(
	SELECT type::CHARACTER VARYING AS data_type,
		'Market Share QSD'::CHARACTER VARYING AS Datasource,
		'Brand'::CHARACTER VARYING AS Data_level,
		'Market Share'::CHARACTER VARYING AS KPI,
		period_type AS Period_type,
		fisc_yr,
		cal_yr,
		pstng_per AS fisc_month,
		cal_mo_2 AS cal_month,
		TO_DATE(period_date::TEXT, 'YYYY-MM-DD') AS "fisc_day",
		TO_DATE(period_date::TEXT, 'YYYY-MM-DD') AS "cal_day",
		(EXTRACT(YEAR FROM TO_DATE(period_date::TEXT, 'YYYY-MM-DD'))::TEXT || LPAD(EXTRACT(MONTH FROM TO_DATE(period_date::TEXT, 'YYYY-MM-DD'))::CHARACTER VARYING::TEXT, 3, '0'::CHARACTER VARYING::TEXT)) AS "fisc_per",
		b.destination_cluster AS "cluster",
		b.destination_market AS "ctry_nm",
		b.destination_market AS "sub_country",
		'E-Commerce'::CHARACTER VARYING AS channel,
		NULL AS sub_channel,
		SPLIT_PART(channel, 'Ecommerce ', 2) AS retail_env,
		NULL AS gotomodel,
		NULL AS profit_center,
		NULL AS company_code,
		NULL AS customer_code,
		NULL AS parentcustomer,
		NULL AS banner,
		NULL AS bannerformat,
		NULL AS platform_name,
		NULL AS retailer_name,
		NULL AS retailer_name_english,
		manufacturer AS manufacturer_name,
		CASE 
			WHEN brand_manufacturer_flg = 'J&J'
				THEN 'Y'
			ELSE 'N'
			END AS jj_manufacturer_flag,
		gfo AS prod_hier_l1,
		need_state AS prod_hier_l2,
		category AS prod_hier_l3,
		sub_category AS prod_hier_l4,
		brand AS prod_hier_l5,
		NULL AS prod_hier_l6,
		segment AS prod_hier_l7,
		sub_segment AS prod_hier_l8,
		NULL AS prod_hier_l9,
		NULL AS product_minor_code,
		NULL AS product_minor_name,
		NULL AS material_number,
		NULL AS ean,
		NULL AS retailer_sku_code,
		NULL AS pka_productkey,
		NULL AS pka_productdesc,
		0 AS target_ori,
		mkt_share_value,
		mkt_share_value AS usd_value,
		0 AS salesweight,
		NULL AS lcy_value,
		NULL AS transaction_currency,
		NULL AS to_crncy,
		NULL AS acct_nm,
		NULL AS acct_num,
		text_desc AS account_description_l1,
		NULL AS account_description_l2,
		NULL AS account_description_l3,
		database_date AS "Additional_Information",
		country_strategic_role
	FROM (
		SELECT period_date,
			'Market Share' AS "kpi",
			cluster AS "source_cluster",
			country_geo AS "source_market",
			period_type,
			channel,
			gfo,
			category,
			sub_category,
			segment,
			sub_segment,
			need_state,
			manufacturer,
			brand_manufacturer_flg,
			brand,
			type,
			text_desc,
			country_strategic_role,
			database_date,
			SUM(value) AS mkt_share_value
		FROM edw_market_share_qsd
		WHERE to_date(period_date, 'yyyy-mm-dd') > dateadd(year, (
					SELECT UPPER(filter_value)::INT
					FROM itg_mds_ap_ecom_oneview_config
					WHERE dataset = 'Market Share QSD'
						AND dataset_area = 'Year Range'
					), current_timestamp())
			AND UPPER(period_type) IN (
				SELECT UPPER(filter_value)
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset = 'Market Share QSD'
					AND dataset_area = 'Period Type Filter'
				)
			AND UPPER(region) IN (
				SELECT UPPER(filter_value)
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset = 'Market Share QSD'
					AND dataset_area = 'Region Filter'
				)
			AND UPPER(type) IN (
				SELECT UPPER(filter_value)
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset = 'Market Share QSD'
					AND dataset_area = 'Value Type Filter'
				)
			AND UPPER(country_geo) NOT IN (
				SELECT UPPER(filter_value)
				FROM itg_mds_ap_ecom_oneview_config
				WHERE dataset = 'Market Share QSD'
					AND dataset_area = 'Market Exclusion'
				)
			AND UPPER(country_geo) NOT LIKE '%OTAL%'
			AND country_geo NOT LIKE '%+%'
			AND UPPER(channel) LIKE '%ECOMMERCE%'
		GROUP BY period_date,
			cluster,
			country_geo,
			period_type,
			channel,
			gfo,
			category,
			sub_category,
			segment,
			sub_segment,
			need_state,
			manufacturer,
			brand_manufacturer_flg,
			brand,
			type,
			text_desc,
			country_strategic_role,
			database_date
		) mkt_share
	LEFT JOIN edw_calendar_dim cal ON mkt_share.period_date = cal.cal_day
	LEFT JOIN itg_mds_ap_sales_ops_map b ON UPPER(mkt_share."source_cluster") = UPPER(b.source_cluster)
		AND UPPER(mkt_share."source_market") = UPPER(b.source_market)
		AND dataset = 'Market Share QSD'
),
insert15 as(
	SELECT 'Act' AS "data_type",
		'Pharmacy' AS "datasource",
		'SKU' AS "data_level",
		'Offtake' AS "kpi",
		'Continuous' AS "period_type",
		jj_year AS "fisc_year",
		cal_year AS "cal_year",
		jj_mnth AS "fisc_month",
		cal_mnth AS "cal_month",
		week_end_dt AS "fisc_day",
		week_end_dt AS "cal_day",
		fisc_per AS "fisc_yr_per",
		'Pacific' AS "cluster",
		'Pacific' AS "market",
		'Pacific' AS "sub_market",
		'E-Commerce' AS "channel",
		'E-Commerce' AS "sub_channel",
		'E-Commerce' AS "retail_environment",
		NULL AS "go_to_model",
		NULL AS "profit_center",
		NULL AS "company_code",
		NULL AS "sap_customer_code",
		NULL AS "sap_customer_name",
		NULL AS "banner",
		NULL AS "banner_format",
		cust_group AS "platform_name",
		cust_group AS "retailer_name",
		cust_group AS "retailer_name_english",
		manufacturer AS "manufacturer_name",
		'Y' AS "jj_manufacturer_flag",
		gcph_franchise AS "prod_hier_l1",
		gcph_needstate AS "prod_hier_l2",
		gcph_category AS "prod_hier_l3",
		gcph_subcategory AS "prod_hier_l4",
		gcph_brand AS "prod_hier_l5",
		gcph_subbrand AS "prod_hier_l6",
		gcph_variant AS "prod_hier_l7",
		put_up_desc AS "prod_hier_l8",
		prod_desc AS "prod_hier_l9",
		NULL AS "product_minor_code",
		NULL AS "prod_minor_name",
		prod_sapbw_code AS "material_number",
		prod_ean AS "ean",
		product_probe_id AS "retailer_sku_code",
		pka_rootcode AS "product_key",
		pka_rootcodedes AS "product_key_description",
		NULL AS "target_value",
		unit_online AS "actual_value",
		usd_sales_online AS "value_usd",
		aud_sales_online AS "value_lcy",
		0 AS salesweight,
		'AUD' AS "from_crncy",
		'USD' AS "to_crncy",
		NULL AS "account_number",
		NULL AS "account_name",
		NULL AS "account_description_l1",
		NULL AS "account_description_l2",
		NULL AS "account_description_l3",
		NULL AS "additional_information",
		NULL AS "ppm_role"
	FROM edw_pharmacy_ecommerce_analysis a
	LEFT JOIN edw_calendar_dim cal ON cal.cal_day = a.week_end_dt
	LEFT JOIN (
		SELECT LTRIM(matl_num, '0') AS "matl_num",
			put_up_desc,
			pka_rootcode,
			pka_rootcodedes
		FROM edw_product_key_attributes
		WHERE UPPER(ctry_nm) IN ('AUSTRALIA', 'NEW ZEALAND')
			AND matl_type_cd = 'FERT'
		GROUP BY 1,
			2,
			3,
			4
		) b ON LTRIM(a.prod_sapbw_code, '0') = b."matl_num"
	WHERE OWNER = 'J&J'
),
transformed as(
	select * from insert1
	union all
	select * from insert2
    union all
	select * from insert3
	union all
	select * from insert4
	union all
	select * from insert5
	union all
	select * from insert6
	union all
	select * from insert7
	union all
	select * from insert8
	union all
	select * from insert9
	union all
	select * from insert10
	union all
	select * from insert11
    union all
	select * from insert12
    union all
	select * from insert13
    union all
	select * from insert14
    union all
	select * from insert15
),
final as(
    select
        data_type::varchar(20) as data_type,
        Datasource::varchar(20) as dataset,
        Data_level::varchar(20) as data_level,
        KPI::varchar(50) as kpi,	  
        Period_type::varchar(10) as period_type,
        fisc_year::varchar(10) as fisc_year,
        cal_year::varchar(10) as cal_year,
        fisc_month::varchar(20) as fisc_month,
        cal_month::varchar(20) as cal_month,
        to_date(fisc_day) as fisc_day,
        to_date(cal_day) as cal_day,
        fisc_yr_per::varchar(50) as fisc_yr_per,
        "cluster"::varchar(100) as cluster,
        ctry_nm::varchar(40) as market,
        sub_country::varchar(40) as sub_market,
        channel::varchar(50) as channel,
        "sub channel"::varchar(50) as sub_channel,
        retail_env::varchar(50) as retail_environment ,
        "go to model"::varchar(50) as go_to_model,
        profit_center::varchar(50) as profit_center,
        company_code::varchar(20) as company_code,
        customer_code::varchar(50) as sap_customer_code ,
        "parent customer"::varchar(250) as sap_customer_name,
        banner::varchar(50) as banner,
        "banner format"::varchar(50) as banner_format,
        platform_name::varchar(100) as platform_name,
        retailer_name::varchar(200) as retailer_name,
        retailer_name_english::varchar(200) as retailer_name_english,
        manufacturer_name::varchar(200) as manufacturer_name,
        jj_manufacturer_flag::varchar(10) as jj_manufacturer_flag,
        prod_hier_l1::varchar(200) as prod_hier_l1,
        prod_hier_l2::varchar(200) as prod_hier_l2,
        prod_hier_l3::varchar(200) as prod_hier_l3,
        prod_hier_l4::varchar(200) as prod_hier_l4,
        prod_hier_l5::varchar(200) as prod_hier_l5,
        prod_hier_l6::varchar(200) as prod_hier_l6,
        prod_hier_l7::varchar(200) as prod_hier_l7,
        prod_hier_l8::varchar(200) as prod_hier_l8,
        prod_hier_l9::varchar(1000) as prod_hier_l9,
        product_minor_code::varchar(30) as product_minor_code,
        product_minor_name::varchar(200) as product_minor_name,
        material_number::varchar(50) as material_number,
        ean::varchar(20) as ean,
        reailer_sku_code::varchar(200) as retailer_sku_code,
        pka_product_key::varchar(100) as product_key,
        pka_product_key_description::varchar(255) as product_key_description,
        target_ori::number(38,18) as target_value,
        value::varchar(30) as actual_value,
        usd_value::number(38,18) as value_usd,
        lcy_value::number(38,18) as value_lcy,
        salesweight::number(38,18) as salesweight,
        from_crncy::varchar(5) as from_crncy,
        to_crncy::varchar(5) as to_crncy,
        acct_nm::varchar(50) as account_number,
        acct_num::varchar(200) as account_name,
        ciw_desc::varchar(200) as account_description_l1,
        ciw_bucket::varchar(200) as account_description_l2,
        csw_desc::varchar(300) as account_description_l3,
        "Additional_Information"::varchar(1000) as additional_information,
        ppm_role::varchar(100) as ppm_role
    from transformed
)
select * from final
