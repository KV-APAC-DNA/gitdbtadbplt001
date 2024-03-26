with itg_vn_dms_product_dim as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_DMS_PRODUCT_DIM
),
edw_vw_vn_mt_dist_products as (
    select * from DEV_DNA_CORE.VNMEDW_INTEGRATION.EDW_VW_VN_MT_DIST_PRODUCTS
),
itg_mds_vn_allchannel_siso_target_sku as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_MDS_VN_ALLCHANNEL_SISO_TARGET_SKU
),
edw_material_dim as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_MATERIAL_DIM
),
edw_gch_producthierarchy as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_GCH_PRODUCTHIERARCHY
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_copa_trans_fact as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_COPA_TRANS_FACT
),
itg_query_parameters as (
    select * from DEV_DNA_CORE.SGPITG_INTEGRATION.ITG_QUERY_PARAMETERS
),
edw_company_dim as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_COMPANY_DIM
),
v_edw_customer_sales_dim as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.V_EDW_CUSTOMER_SALES_DIM
),
edw_vw_vn_billing_fact as (
    select * from DEV_DNA_CORE.VNMEDW_INTEGRATION.EDW_VW_VN_BILLING_FACT
),
edw_crncy_exch_rates as (
    select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_CRNCY_EXCH_RATES
),
edw_vn_si_st_so_details as (
    select * from DEV_DNA_CORE.VNMEDW_INTEGRATION.EDW_VN_SI_ST_SO_DETAILS
),
itg_vn_dms_distributor_dim as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_DMS_DISTRIBUTOR_DIM
),
itg_vn_dms_customer_dim as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_DMS_CUSTOMER_DIM
),
edw_vw_vn_mt_sell_in_analysis as (
    select * from DEV_DNA_CORE.VNMEDW_INTEGRATION.EDW_VW_VN_MT_SELL_IN_ANALYSIS
),
itg_vn_mt_sellin_dksh as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_DKSH
),
itg_vn_mt_sellin_dksh_history as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_DKSH_HISTORY
),
edw_vw_vn_mt_dist_customers as (
    select * from DEV_DNA_CORE.VNMEDW_INTEGRATION.EDW_VW_VN_MT_DIST_CUSTOMERS
),
itg_vn_mt_customer_sales_organization as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_CUSTOMER_SALES_ORGANIZATION
),
itg_mds_vn_ecom_target as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_MDS_VN_ECOM_TARGET
),
itg_vn_oneview_otc as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_ONEVIEW_OTC
),

prod_dim as 
(
    select 
        prod.sap_code as sap_code,
        rpd.matl_desc as sap_matl_name,
        rpd.pka_franchise_desc as franchise,
        gcph.gcph_brand || ' / ' || gcph.gcph_segment as brand,
        gcph.gcph_variant as variant,
        rpd.pka_product_key_description as product_group,
        case when gcph.gcph_brand = 'Johnson''s Baby' then 'JB' 
            when gcph.gcph_brand is null or gcph.gcph_brand = '' then NULL
            else  'NON-JB' end as group_jb
    from
    (
        select distinct productcodesap  as sap_code from itg_vn_dms_product_dim  where length(productcodesap) <> 0
        union 
        select distinct jnj_sap_code as sap_code from edw_vw_vn_mt_dist_products where length(jnj_sap_code) <> 0
        union 
        select distinct cast (jnj_code as varchar ) as sap_code from  itg_mds_vn_allchannel_siso_target_sku
    ) prod
    left join edw_material_dim rpd on ltrim(rpd.matl_num, 0) = prod.sap_code
    left join edw_gch_producthierarchy gcph on ltrim(gcph.materialnumber, 0) = prod.sap_code
),

time_dim as
(
    SELECT "year",
       substring(qrtr,6,7) as qrtr,
       mnth_id,
       cal_date,
       mnth_no,
       mnth_wk_no,
       ROW_NUMBER() OVER (PARTITION BY mnth_id ORDER BY cal_date) AS mnth_day
    FROM edw_vw_os_time_dim
),

ecom_hist as 
(
    select timedim.mnth_id, 
           (sum(case when ship_to = '624097' then grs_trd_sls else 0 end)/sum(grs_trd_sls)) as ecom_hist_pct
    from edw_vw_vn_billing_fact 
    left join edw_vw_os_time_dim timedim ON bill_dt = timedim.cal_date
    where cntry_key = 'VN' AND sold_to = '133806'
      AND bill_type in ('ZF2V', 'S1') AND bill_qty_pc <> 0 
    group by 1 
    having sum(case when ship_to = '624097' then grs_trd_sls else 0 end)>0
),
 
-----------------------------Sell-In Actual--------------------------------------
cte1 as
(	   
 SELECT 'Sell-In Actual' AS data_type,
      CASE
        WHEN LTRIM(copa.cust_num,'0') = qry_param.parameter_name
        THEN SUBSTRING(qry_param.country_code, 9)
      	ELSE 'MT'
      END AS channel,
      CASE
        WHEN LTRIM(copa.cust_num,'0') = qry_param.parameter_name
        THEN qry_param.parameter_value
      	ELSE 'MTD'
      END AS sub_channel,
      copa.fisc_yr AS jj_year,
      time_dim.qrtr AS jj_qrtr,
      CAST(copa.caln_yr_mo AS varchar(23)) AS jj_mnth_id,
      CAST(SUBSTRING(copa.caln_yr_mo, 5,2) AS INTEGER) AS jj_mnth_no,
      time_dim.mnth_wk_no AS jj_mnth_wk_no,
      CAST(SUBSTRING(copa.caln_day, 7,2) AS BIGINT) AS jj_mnth_day,
      NULL AS mapped_spk, 
      NULL AS dstrbtr_grp_cd, 
      NULL AS dstrbtr_name, 
      LTRIM(copa.cust_num, 0) AS sap_sold_to_code,
      prod_dim.sap_code AS sap_matl_num,
      prod_dim.sap_matl_name,
      NULL AS dstrbtr_matl_num,
      NULL AS dstrbtr_matl_name,
      NULL AS bar_code,
      NULL AS customer_code,
      cus_sales_extn."parent customer" AS customer_name,
      copa.invoice_date,
      NULL AS salesman,
      NULL AS salesman_name,
      copa.gts_incl_dm AS si_gts_val,
      copa.gts_excl_dm AS si_gts_excl_dm_val,
      copa.nts AS si_nts_val,
      NULL AS si_mnth_tgt_by_sku,
      NULL AS so_net_trd_sls_loc,
      NULL AS so_net_trd_sls_usd,
      NULL AS so_mnth_tgt,
      NULL AS so_avg_wk_tgt,
      NULL AS so_mnth_tgt_by_sku,
      NULL AS zone_manager_id,
      NULL AS zone_manager_name,
      NULL AS "zone",
      NULL AS province,
      NULL AS region,
      NULL AS shop_type,
      NULL AS mt_sub_channel,
      NULL AS retail_environment,
      NULL AS group_account,
      NULL AS account,
      prod_dim.franchise,
      prod_dim.brand,
      prod_dim.variant,
      prod_dim.product_group,
      prod_dim.group_jb,
      qry_param.parameter_value,
      copa.caln_yr_mo
FROM 
(
SELECT   
	   ltrim(cust_num, 0) AS sap_sold_to_code,
	   TO_DATE(caln_day, 'YYYYMMDD') AS invoice_date,
	   amt_obj_crncy,
	   matl_num,
	   co_cd,
	   sls_org,
	   dstr_chnl,
	   div,
	   cust_num,
	   caln_yr_mo,
	   ctry_key,
	   fisc_yr,
	   caln_day,
	   SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE NULL END) AS gts_incl_dm,
       SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE 0 END) -SUM(CASE WHEN acct_hier_shrt_desc = 'HDPM' THEN amt_obj_crncy ELSE 0 END) AS gts_excl_dm_temp,
	   CASE WHEN gts_excl_dm_temp != 0 THEN gts_excl_dm_temp ELSE NULL END AS gts_excl_dm,
	   SUM(CASE WHEN acct_hier_shrt_desc = 'NTS' THEN amt_obj_crncy ELSE NULL END) AS nts
	   FROM edw_copa_trans_fact 
       where UPPER(ctry_key) in ('VN')
	   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) copa
  INNER JOIN time_dim ON copa.invoice_date = time_dim.cal_date 
  LEFT JOIN prod_dim ON prod_dim.sap_code = LTRIM(copa.matl_num, '0')    
  FULL JOIN itg_query_parameters qry_param ON LTRIM(copa.cust_num,'0') = qry_param.parameter_name 
  AND LEFT(qry_param.country_code,7) = 'VN_COPA' 
  LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
  LEFT JOIN v_edw_customer_sales_dim cus_sales_extn
         ON copa.sls_org = cus_sales_extn.sls_org
        AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
        AND copa.div = cus_sales_extn.div
        AND copa.cust_num = cus_sales_extn.cust_num
      WHERE 
            (
                si_gts_val IS NOT NULL 
  	            OR si_gts_excl_dm_val IS NOT NULL
  	            OR si_nts_val IS NOT NULL
            )
),

cte1_filtered as
(
select * from cte1
where parameter_value = 'MTI' and jj_mnth_id in (select distinct CAST(caln_yr_mo AS varchar(23)) as jj_mnth_id from edw_copa_trans_fact where jj_mnth_id not between '202105' and '202209')
union all
select * from cte1
where (parameter_value != 'MTI' or parameter_value is null) and jj_mnth_id = CAST(caln_yr_mo AS varchar(23))
),

cte2 as 
(   
---------------------------------------------------------------OTC Sellin Actual--------------------------------------
SELECT 'Sell-In Actual' AS data_type,
      CASE
        WHEN LTRIM(copa.cust_num,'0') = qry_param.parameter_name
        THEN SUBSTRING(qry_param.country_code, 9)
      	ELSE 'MT'
      END AS channel,
      CASE
        WHEN LTRIM(copa.cust_num,'0') = qry_param.parameter_name
        THEN qry_param.parameter_value
      	ELSE 'MTD'
      END AS sub_channel,
      copa.fisc_yr AS jj_year,
      time_dim.qrtr AS jj_qrtr,
      CAST(copa.caln_yr_mo AS varchar(23)) AS jj_mnth_id,
      CAST(SUBSTRING(copa.caln_yr_mo, 5,2) AS INTEGER) AS jj_mnth_no,
      time_dim.mnth_wk_no AS jj_mnth_wk_no,
      CAST(SUBSTRING(copa.caln_day, 7,2) AS BIGINT) AS jj_mnth_day,
      NULL AS mapped_spk, 
      NULL AS dstrbtr_grp_cd, 
      NULL AS dstrbtr_name, 
      LTRIM(copa.cust_num, 0) AS sap_sold_to_code,
      prod_dim.sap_code AS sap_matl_num,
      prod_dim.sap_matl_name,
      NULL AS dstrbtr_matl_num,
      NULL AS dstrbtr_matl_name,
      NULL AS bar_code,
      NULL AS customer_code,
      cus_sales_extn."parent customer" AS customer_name,
      copa.invoice_date,
      NULL AS salesman,
      NULL AS salesman_name,
      copa.gts_incl_dm *  exch_rate.ex_rt * exch_rate.to_ratio/exch_rate.from_ratio AS si_gts_val,
      copa.gts_excl_dm  * exch_rate.ex_rt *exch_rate.to_ratio/exch_rate.from_ratio AS si_gts_excl_dm_val,
      copa.nts  * exch_rate.ex_rt* exch_rate.to_ratio/exch_rate.from_ratio AS si_nts_val,
      NULL AS si_mnth_tgt_by_sku,
      NULL AS so_net_trd_sls_loc,
      NULL AS so_net_trd_sls_usd,
      NULL AS so_mnth_tgt,
      NULL AS so_avg_wk_tgt,
      NULL AS so_mnth_tgt_by_sku,
      NULL AS zone_manager_id,
      NULL AS zone_manager_name,
      NULL AS "zone",
      NULL AS province,
      NULL AS region,
      NULL AS shop_type,
      NULL AS mt_sub_channel,
      NULL AS retail_environment,
      NULL AS group_account,
      NULL AS account,
      prod_dim.franchise,
      prod_dim.brand,
      prod_dim.variant,
      prod_dim.product_group,
      prod_dim.group_jb,
      qry_param.parameter_value,
      copa.caln_yr_mo
FROM 
(
SELECT   
	   ltrim(cust_num, 0) AS sap_sold_to_code,
	   TRY_TO_DATE(caln_day, 'YYYYMMDD') AS invoice_date,
	   amt_obj_crncy,
	   matl_num,
	   co_cd,
	   sls_org,
	   dstr_chnl,
	   div,
	   cust_num,
	   caln_yr_mo,
	   ctry_key,
	   fisc_yr,
	   caln_day,
	   SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE NULL END) AS gts_incl_dm,
       SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE 0 END) -SUM(CASE WHEN acct_hier_shrt_desc = 'HDPM' THEN amt_obj_crncy ELSE 0 END) AS gts_excl_dm_temp,
	   (CASE WHEN gts_excl_dm_temp != 0 THEN gts_excl_dm_temp ELSE NULL END )  AS gts_excl_dm,
	   SUM(CASE WHEN acct_hier_shrt_desc = 'NTS' THEN amt_obj_crncy ELSE NULL END)  AS nts
	   FROM edw_copa_trans_fact 
       where UPPER(ctry_key) in ('SG')
	   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) copa
  INNER JOIN time_dim ON copa.invoice_date = time_dim.cal_date 
  LEFT JOIN (SELECT derived_table1."year" || derived_table1.mnth AS mnth_id,
                 derived_table1.ex_rt, derived_table1.from_ratio,derived_table1.to_ratio
            FROM (SELECT edw_crncy_exch_rates.fisc_yr_per,
                      substring(edw_crncy_exch_rates.fisc_yr_per::CHARACTER VARYING::TEXT,1,4) AS "year",
                          substring(edw_crncy_exch_rates.fisc_yr_per::CHARACTER VARYING::TEXT,6,2) AS mnth,
                           edw_crncy_exch_rates.ex_rt,
                           edw_crncy_exch_rates.from_ratio
                           ,edw_crncy_exch_rates.to_ratio
                   FROM edw_crncy_exch_rates
                   WHERE edw_crncy_exch_rates.from_crncy::TEXT = 'SGD'::CHARACTER VARYING::TEXT
                   AND   edw_crncy_exch_rates.to_crncy::TEXT = 'VND'::CHARACTER VARYING::TEXT) derived_table1
                   ) exch_rate
                    ON time_dim.mnth_id::NUMERIC::NUMERIC (18,0) = exch_rate.mnth_id::NUMERIC::NUMERIC (18,0)
  LEFT JOIN prod_dim ON prod_dim.sap_code = LTRIM(copa.matl_num, '0')    
  FULL JOIN itg_query_parameters qry_param ON LTRIM(copa.cust_num,'0') = qry_param.parameter_name 
  AND LEFT(qry_param.country_code,7) = 'VN_COPA' 
  LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
  LEFT JOIN v_edw_customer_sales_dim cus_sales_extn
         ON copa.sls_org = cus_sales_extn.sls_org
        AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
        AND copa.div = cus_sales_extn.div
        AND copa.cust_num = cus_sales_extn.cust_num
  WHERE 
   (si_gts_val IS NOT NULL 
  	   OR si_gts_excl_dm_val IS NOT NULL
  	   OR si_nts_val IS NOT NULL)

),

cte2_filtered as
(
select * from cte2
where parameter_value = 'MTI' and jj_mnth_id in (select distinct CAST(caln_yr_mo AS varchar(23)) as jj_mnth_id from edw_copa_trans_fact where jj_mnth_id not between '202105' and '202209')
union all 
select * from cte2
where (parameter_value != 'MTI' or parameter_value is null) and jj_mnth_id = CAST(caln_yr_mo AS varchar(23))
),

cte3 as
(
    SELECT 'Sell-In Actual' AS data_type,
       'ECOM' AS channel,
	   'ECOM' AS sub_channel,
	   copa.fisc_yr AS jj_year,
       time_dim.qrtr AS jj_qrtr,
       CAST(copa.caln_yr_mo AS varchar(23)) AS jj_mnth_id,
	   CAST(SUBSTRING(copa.caln_yr_mo, 5,2) AS INTEGER) AS jj_mnth_no,
       time_dim.mnth_wk_no AS jj_mnth_wk_no,
	   CAST(SUBSTRING(copa.caln_day, 7,2) AS BIGINT) AS jj_mnth_day,
       NULL AS mapped_spk, 
       NULL AS dstrbtr_grp_cd, 
       NULL AS dstrbtr_name, 
       LTRIM(copa.cust_num, 0) AS sap_sold_to_code,
	   prod_dim.sap_code AS sap_matl_num,
       prod_dim.sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       NULL AS bar_code,
       NULL AS customer_code,
	   cus_sales_extn."parent customer" AS customer_name,
  	   copa.invoice_date,
  	   NULL AS salesman,
       NULL AS salesman_name,
	   ecom_hist_pct * copa.gts_incl_dm AS si_gts_val,
  	   ecom_hist_pct * copa.gts_excl_dm AS si_gts_excl_dm_val,
  	   ecom_hist_pct * copa.nts AS si_nts_val,
  	   NULL AS si_mnth_tgt_by_sku,
       NULL AS so_net_trd_sls_loc,
       NULL AS so_net_trd_sls_usd,
       NULL AS so_mnth_tgt,
       NULL AS so_avg_wk_tgt,
       NULL AS so_mnth_tgt_by_sku,
       NULL AS zone_manager_id,
       NULL AS zone_manager_name,
       NULL AS "zone",
       NULL AS province,
	   NULL AS region,
       NULL AS shop_type,
       NULL AS mt_sub_channel,
       NULL AS retail_environment,
       NULL AS group_account,
       NULL AS account,
	   prod_dim.franchise,
       prod_dim.brand,
       prod_dim.variant,
       prod_dim.product_group,
       prod_dim.group_jb,
       null as parameter_value,
       copa.caln_yr_mo
FROM 
      (
       SELECT   
	   CASE LTRIM(cust_num,'0')
         WHEN '135463' THEN 'TD002-Tiến Thành'
         WHEN '135462' THEN 'TD001-Dương Anh'
         WHEN '133806' THEN 'MTI'
         WHEN '138023' THEN 'ECOM'
         ELSE 'MTD'
       END AS sub_channel,
	   LTRIM(cust_num, 0) AS sap_sold_to_code,
	   TO_DATE(caln_day, 'YYYYMMDD') AS invoice_date,
	   amt_obj_crncy,
	   matl_num,
	   co_cd,
	   sls_org,
	   dstr_chnl,
	   div,
	   cust_num,
	   caln_yr_mo,
	   ctry_key,
	   fisc_yr,
	   caln_day,
	   SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE NULL END) AS gts_incl_dm,
       SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE 0 END) -SUM(CASE WHEN acct_hier_shrt_desc = 'HDPM' THEN amt_obj_crncy ELSE 0 END) AS gts_excl_dm_temp,
       CASE WHEN gts_excl_dm_temp != 0 THEN gts_excl_dm_temp ELSE NULL END AS gts_excl_dm,
       SUM(CASE WHEN acct_hier_shrt_desc = 'NTS' THEN amt_obj_crncy ELSE NULL END) AS nts
	   FROM edw_copa_trans_fact 
       WHERE UPPER(ctry_key) = 'VN' 
	   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
       ) copa
       LEFT JOIN ecom_hist ON copa.caln_yr_mo = ecom_hist.mnth_id
       INNER JOIN time_dim ON copa.invoice_date = time_dim.cal_date 
       LEFT JOIN prod_dim ON prod_dim.sap_code = LTRIM(copa.matl_num, '0')    
       LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
       LEFT JOIN v_edw_customer_sales_dim cus_sales_extn
           ON copa.sls_org = cus_sales_extn.sls_org
          AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
          AND copa.div = cus_sales_extn.div
          AND copa.cust_num = cus_sales_extn.cust_num
       WHERE copa.sub_channel = 'MTI'
	   AND (si_gts_val IS NOT NULL 
  	   OR si_gts_excl_dm_val IS NOT NULL
  	   OR si_nts_val IS NOT NULL)
),

cte4 as 
(
    SELECT 'Sell-In Actual' AS data_type,
       'MT' AS channel,
	   copa.sub_channel,
	   copa.fisc_yr AS jj_year,
       time_dim.qrtr AS jj_qrtr,
       CAST(copa.caln_yr_mo AS varchar(23)) AS jj_mnth_id,
	   CAST(SUBSTRING(copa.caln_yr_mo, 5,2) AS INTEGER) AS jj_mnth_no,
       time_dim.mnth_wk_no AS jj_mnth_wk_no,
	   CAST(SUBSTRING(copa.caln_day, 7,2) AS BIGINT) AS jj_mnth_day,
       NULL AS mapped_spk, 
       NULL AS dstrbtr_grp_cd, 
       NULL AS dstrbtr_name, 
       LTRIM(copa.cust_num, 0) AS sap_sold_to_code,
	   prod_dim.sap_code AS sap_matl_num,
       prod_dim.sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       NULL AS bar_code,
       NULL AS customer_code,
	   cus_sales_extn."parent customer" AS customer_name,
  	   copa.invoice_date,
  	   NULL AS salesman,
       NULL AS salesman_name,
  	   (1-ecom_hist_pct) * copa.gts_incl_dm AS si_gts_val,
  	   (1-ecom_hist_pct) * copa.gts_excl_dm AS si_gts_excl_dm_val,
  	   (1-ecom_hist_pct) * copa.nts AS si_nts_val,
  	   NULL AS si_mnth_tgt_by_sku,
       NULL AS so_net_trd_sls_loc,
       NULL AS so_net_trd_sls_usd,
       NULL AS so_mnth_tgt,
       NULL AS so_avg_wk_tgt,
       NULL AS so_mnth_tgt_by_sku,
       NULL AS zone_manager_id,
       NULL AS zone_manager_name,
       NULL AS "zone",
       NULL AS province,
	   NULL AS region,
       NULL AS shop_type,
       NULL AS mt_sub_channel,
       NULL AS retail_environment,
       NULL AS group_account,
       NULL AS account,
	   prod_dim.franchise,
       prod_dim.brand,
       prod_dim.variant,
       prod_dim.product_group,
       prod_dim.group_jb,
       null as parameter_value,
       copa.caln_yr_mo
FROM 
    (
    SELECT   
	   CASE LTRIM(cust_num,'0')
         WHEN '135463' THEN 'TD002-Tiến Thành'
         WHEN '135462' THEN 'TD001-Dương Anh'
         WHEN '133806' THEN 'MTI'
         WHEN '138023' THEN 'ECOM'
         ELSE 'MTD'
       END AS sub_channel,
	   LTRIM(cust_num, 0) AS sap_sold_to_code,
	   TO_DATE(caln_day, 'YYYYMMDD') AS invoice_date,
	   amt_obj_crncy,
	   matl_num,
	   co_cd,
	   sls_org,
	   dstr_chnl,
	   div,
	   cust_num,
	   caln_yr_mo,
	   ctry_key,
	   fisc_yr,
	   caln_day,
	   SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE NULL END) AS gts_incl_dm,
       SUM(CASE WHEN acct_hier_shrt_desc = 'GTS' THEN amt_obj_crncy ELSE 0 END) -SUM(CASE WHEN acct_hier_shrt_desc = 'HDPM' THEN amt_obj_crncy ELSE 0 END) AS gts_excl_dm_temp,
       CASE WHEN gts_excl_dm_temp != 0 THEN gts_excl_dm_temp ELSE NULL END AS gts_excl_dm,
       SUM(CASE WHEN acct_hier_shrt_desc = 'NTS' THEN amt_obj_crncy ELSE NULL END) AS nts
	   FROM edw_copa_trans_fact 
       WHERE UPPER(ctry_key) = 'VN'
	   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
       ) copa
	   LEFT JOIN ecom_hist ON copa.caln_yr_mo = ecom_hist.mnth_id
	   INNER JOIN time_dim ON copa.invoice_date = time_dim.cal_date 
	   LEFT JOIN prod_dim ON prod_dim.sap_code = LTRIM(copa.matl_num, '0')    
	   LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
	   LEFT JOIN v_edw_customer_sales_dim cus_sales_extn
          ON copa.sls_org = cus_sales_extn.sls_org
          AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
          AND copa.div = cus_sales_extn.div
          AND copa.cust_num = cus_sales_extn.cust_num
    WHERE copa.sub_channel = 'MTI'
	AND (si_gts_val IS NOT NULL 
  	   OR si_gts_excl_dm_val IS NOT NULL
  	   OR si_nts_val IS NOT NULL)
),

cte5 as
(
    select 'Sell-Out Actual' as data_type,
            'GT' as channel,
            so.territory_dist as sub_channel,
            so.jj_year,
            time_dim.qrtr as jj_qrtr,
            so.jj_mnth_id,
            so.jj_mnth_no,
            so.jj_mnth_wk_no,
            time_dim.mnth_day as jj_mnth_day,
            so.distributor_id_report as mapped_spk,
            so.dstrbtr_grp_cd  ,
            so.dstrbtr_name ,
            so.sap_sold_to_code,
            so.sap_matl_num,
            prod_dim.sap_matl_name,
            so.dstrbtr_matl_num,
            so.dstrbtr_matl_name,
            null as bar_code,
            so.cust_cd as customer_code,
            cust.outlet_name as customer_name,
            so.bill_date as invoice_date,
            so.slsmn_cd as salesman,
            so.slsmn_nm as salesman_name,
            so.si_gts_val,
            null as si_gts_excl_dm_val,
            so.si_nts_val,
            null as si_mnth_tgt_by_sku,
            so.so_net_trd_sls as so_net_trd_sls_loc,
            so.so_net_trd_sls*so.exchg_rate as so_net_trd_sls_usd,
            so.so_mnth_tgt,
            so.so_avg_wk_tgt,
            null as so_mnth_tgt_by_sku,
            so.asm_id as zone_manager_id,
            so.asm_name as zone_manager_name,
            null as "zone",
            dist.province,
            so.dstrb_region as region,
            cust.shop_type,
            null as mt_sub_channel,
            null as retail_environment,
            null as group_account,
            null as account,
            prod_dim.franchise,
            prod_dim.brand,
            prod_dim.variant,
            prod_dim.product_group,
            prod_dim.group_jb,
            null as parameter_value,
            null as caln_yr_mo
		from edw_vn_si_st_so_details  so
		inner join time_dim on time_dim.cal_date = so.bill_date
		--left join os_itg.itg_vn_dms_distributor_dim dist
        /*commented the above join and replacing with the new join that fetches distributo_IDs with the latest mapped_spk*/
        left join (
              SELECT territory_dist,
                     mapped_spk,
                     dstrbtr_id,
                     dstrbtr_type,
                     dstrbtr_name,
                     asmid,
                     region,
			         province
             --sls.asm_name
              FROM       
              (
                SELECT territory_dist,
                     mapped_spk,
                     dstrbtr_id,
                     dstrbtr_type,
                     dstrbtr_name,
                     asm_id as asmid,
                     region,
			        province,
              Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
              FROM itg_vn_dms_distributor_dim
              ) where rn = 1) dist	
		    on dist.dstrbtr_id = so.dstrbtr_grp_cd
		left join itg_vn_dms_customer_dim cust
		    on cust.outlet_id = so.cust_cd
		left join prod_dim on prod_dim.sap_code = so.sap_matl_num
		where  (so_net_trd_sls <> 0)
),

cte6 as 
(
    select 'Sell-Out Target' as data_type,
            'GT' as channel,
            so_tgt.territory_dist as sub_channel,
            so_tgt.jj_year,
            substring(so_tgt.jj_qrtr,6,7) as jj_qrtr,
            so_tgt.jj_mnth_id,
            so_tgt.jj_mnth_no,
            so_tgt.jj_mnth_wk_no,
            null as jj_mnth_day,
            so_tgt.distributor_id_report as mapped_spk,
            so_tgt.dstrbtr_grp_cd  ,
            so_tgt.dstrbtr_name ,
            so_tgt.sap_sold_to_code,
            null as sap_matl_num,
            null as sap_matl_name,
            null as dstrbtr_matl_num,
            null as dstrbtr_matl_name,
            null as bar_code,
            null as customer_code,
            null as customer_name,
            null as invoice_date,
            so_tgt.slsmn_cd as salesman,
            so_tgt.slsmn_nm as salesman_name,
            null as si_gts_val,
            null as si_gts_excl_dm_val,
            null as si_nts_val,
            null as si_mnth_tgt_by_sku,
            null as so_net_trd_sls_loc,
            null as so_net_trd_sls_usd,
            so_tgt.mnth_tgt as so_mnth_tgt,
            so_tgt.wk_tgt as so_avg_wk_tgt,
            null as so_mnth_tgt_by_sku,
            so_tgt.asm_id as zone_manager_id,
            so_tgt.asm_name as zone_manager_name,
            null as "zone",
            dist.province,
            dist.region,
            null as shop_type,
            null as mt_sub_channel,
            null as retail_environment,
            null as group_account,
            null as account,
            null as franchise,
            null as brand,
            null as variant,
            null as product_group,
            null as group_jb,
            null as parameter_value,
            null as caln_yr_mo
		 from
		(
        select jj_year
            ,jj_qrtr
            ,jj_mnth_id
            ,jj_mnth_no
            ,jj_mnth_wk_no
            ,territory_dist
            ,distributor_id_report
            ,dstrbtr_grp_cd
            ,dstrbtr_name
            ,sap_sold_to_code
            ,slsmn_cd
            ,slsmn_nm
            ,asm_id 
            ,asm_name
            ,max(so_mnth_tgt) as mnth_tgt
            ,max(so_avg_wk_tgt) as wk_tgt
		from edw_vn_si_st_so_details
		where slsmn_cd is not null
		group by jj_year, jj_qrtr,jj_mnth_id, jj_mnth_no,jj_mnth_wk_no, territory_dist,distributor_id_report,dstrbtr_grp_cd,dstrbtr_name,sap_sold_to_code,
		slsmn_cd,slsmn_nm, asm_id , asm_name
        ) so_tgt
		--left join os_itg.itg_vn_dms_distributor_dim dist
 /*commented the above join and replacing with the new join that fetches distributo_IDs with the latest mapped_spk*/
        left join (
            SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region,
			 province
             --sls.asm_name
            FROM       
            (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
			 province,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
            FROM itg_vn_dms_distributor_dim
            ) where rn = 1) dist 		
		on dist.dstrbtr_id = so_tgt.dstrbtr_grp_cd
		where so_tgt.territory_dist  in('TD001-Dương Anh','TD002-Tiến Thành')
),

cte7 as
(
    select case when gt_sku_tgt.data_type = 'Sell-In' then 'Sell-In Target'
				  else 'Sell-Out Target'
				  end  as data_type,
			   'GT' as channel,
			   gt_sku_tgt.territory_dist as sub_channel,
			   timedim."year" as jj_year,
			   substring(timedim.qrtr,6,7) AS jj_qrtr,
			   timedim.mnth_id AS jj_mnth_id,
			   timedim.mnth_no AS jj_mnth_no,
			   null as jj_mnth_wk_no,
			   null as jj_mnth_day,
			   null as mapped_spk,
			   null as dstrbtr_grp_cd,
			   null as dstrbtr_name,
			   null as sap_sold_to_code,
			   prod.sap_code as sap_matl_num,
			   prod.sap_matl_name,
			   cast(gt_sku_tgt.jnj_code as varchar) as dstrbtr_matl_num,
			   gt_sku_tgt.description as dstrbtr_matl_name,
			   null as bar_code,
			   null as customer_code,
			   null as customer_name,
			   null as invoice_date,
			   null as salesman,
			   null as salesman_name,
			   null as si_gts_val,
			   null as si_gts_excl_dm_val,
			   null as si_nts_val,
			   case when gt_sku_tgt.data_type = 'Sell-In' then gt_sku_tgt.tgt else null end as si_mnth_tgt_by_sku,
			   null as so_net_trd_sls_loc,
			   null as so_net_trd_sls_usd,
			   null as so_mnth_tgt,
			   null as so_avg_wk_tgt,
			   case when gt_sku_tgt.data_type = 'Sell-Out' then gt_sku_tgt.tgt else null end as so_mnth_tgt_by_sku,
			   null as zone_manager_id,
			   null as zone_manager_name,
			   null as "zone",
			   null as province,
			   null as region,
			   null as shop_type,
			   null as mt_sub_channel,
			   null as retail_environment,
			   null as group_account,
			   null as account,
			   nvl(prod.franchise,'NA') as franchise,
			   nvl(prod.brand,'NA') as brand,
			   nvl(prod.variant,'NA') as variant,
			   nvl(prod.product_group,'NA') as product_group,
			   nvl(prod.group_jb,'NA') as group_jb,
               null as parameter_value,
               null as caln_yr_mo
		 from
		    (  
                select cycle 
                ,data_type
                ,td.territory_dist 
                ,tgt
                ,jnj_code 
                ,description 
            from
		    (
                select cycle ,data_type,'TD002' as  territory_dist, gt_tien_thanh*1000000 as tgt, jnj_code, description 
                from itg_mds_vn_allchannel_siso_target_sku 
		        union all 
		        select cycle ,data_type,'TD001' as  territory_dist, gt_duong_anh*1000000 as tgt, jnj_code, description 
                from itg_mds_vn_allchannel_siso_target_sku 
            )sku_tgt 
		        left join 
		        (select distinct territory_dist from itg_vn_dms_distributor_dim) td
		        on substring(td.territory_dist,1,5)= sku_tgt.territory_dist)gt_sku_tgt 
		        inner join 
		        (   
                    SELECT edw_vw_os_time_dim."year",
							edw_vw_os_time_dim.qrtr,
							edw_vw_os_time_dim.mnth_id,
							edw_vw_os_time_dim.mnth_no
					 FROM edw_vw_os_time_dim
					 GROUP BY edw_vw_os_time_dim."year",
							  edw_vw_os_time_dim.qrtr,
							  edw_vw_os_time_dim.mnth_id,
							  edw_vw_os_time_dim.mnth_no
                )timedim
		        on cast(timedim.mnth_id as numeric) = gt_sku_tgt.cycle
		        left join  prod_dim prod on prod.sap_code = cast(gt_sku_tgt.jnj_code as varchar)
		        where gt_sku_tgt.territory_dist in ('TD001-Dương Anh','TD002-Tiến Thành')
),

cte8 as
(
    select case when mt.data_source in ('JNJ','DKSH') then 'Sell-Out Actual'
       else 'Sell-Out Target' end as data_type,
       'MT' as channel,
       case when (mt.data_source = 'DKSH' ) or (mt.data_type = 'DKSH') then 'MTI' 
            when (mt.data_source = 'JNJ' ) or (mt.data_type = 'JNJ')then 'MTD'
            else 'Invalid' end as sub_channel,
       mt.year as jj_year,
       substring(mt.qrtr,6,7) as jj_qrtr,
       mt.mnth_id as jj_mnth_id,
       mt.mnth_no as jj_mnth_no,
       mt.mnth_wk_no as jj_mnth_wk_no,
       time_dim.mnth_day as jj_mnth_day,
       null as mapped_spk,
       null as dstrbtr_grp_cd,
       null as dstrbtr_name,
       null as sap_sold_to_code,
       prod.sap_code as sap_matl_num,
       prod.sap_matl_name ,
       mt.productid as dstrbtr_matl_num,
       mt.product_name as dstrbtr_matl_name,
       mt.barcode,
       mt.custcode as customer_code,
       mt.name as customer_name,
       cast(mt.invoice_date as date) as invoice_date,
       null as salesman,
       null as salesman_name,
       null as si_gts_val,
	   null as si_gts_excl_dm_val,
       null as si_nts_val,
       null as si_mnth_tgt_by_sku,
       case when mt.data_source = 'JNJ' then mt.gts_amt_lcy else mt.sales_amt_lcy end  as so_net_trd_sls_loc,
       case when mt.data_source = 'JNJ'  then mt.gts_amt_usd else mt.sales_amt_usd end as so_net_trd_sls_usd,
       mt.target_lcy as so_mnth_tgt,
       null as so_avg_wk_tgt,
       null as so_mnth_tgt_by_sku,
       null as zone_manager_id,
       mt.kam as zone_manager_name,
       mt.zone,
       mt.province,
       mt.region,
       null as shop_type,
       mt.sub_channel as mt_sub_channel,
       mt.retail_environment,
       mt.group_account,
       mt.account,
       prod.franchise,
       prod.brand,
       prod.variant,
       prod.product_group,
       prod.group_jb,
       null as parameter_value,
       null as caln_yr_mo
    from edw_vw_vn_mt_sell_in_analysis mt
    left join time_dim on time_dim.cal_date = cast(mt.invoice_date as date)
    left join prod_dim prod on
    case when mt.data_source = 'JNJ' then
    prod.sap_code = cast(mt.productid as varchar)
    else 
    prod.sap_code = cast(mt.jnj_sap_code as varchar) end
    where mt.data_source <> 'COOP'
),

cte9 as 
(
    select case when mt_sku_tgt.data_type = 'Sell-In' then 'Sell-In Target'
          else 'Sell-Out Target'
          end  as data_type,
       'MT' as channel,
        mt_sku_tgt.sub_channel as sub_channel,
       timedim."year" as jj_year,
       substring(timedim.qrtr,6,7) AS jj_qrtr,
       timedim.mnth_id AS jj_mnth_id,
       timedim.mnth_no AS jj_mnth_no,
       null as jj_mnth_wk_no,
       null as jj_mnth_day,
       null as mapped_spk,
       null as dstrbtr_grp_cd,
       null as dstrbtr_name,
       null as sap_sold_to_code,
       prod.sap_code as sap_matl_num,
       prod.sap_matl_name,
       cast(mt_sku_tgt.jnj_code as varchar) as dstrbtr_matl_num,
       mt_sku_tgt.description as dstrbtr_matl_name,
       null as bar_code,
       null as customer_code,
       null as customer_name,
       null as invoice_date,
       null as salesman,
       null as salesman_name,
       null as si_gts_val,
	   null as si_gts_excl_dm_val,
       null as si_nts_val,
       case when mt_sku_tgt.data_type = 'Sell-In' then mt_sku_tgt.tgt else null end as si_mnth_tgt_by_sku,
       null as so_net_trd_sls_loc,
       null as so_net_trd_sls_usd,
       null as so_mnth_tgt,
       null as so_avg_wk_tgt,
       case when mt_sku_tgt.data_type = 'Sell-Out' then mt_sku_tgt.tgt else null end as so_mnth_tgt_by_sku,
       null as zone_manager_id,
       null as zone_manager_name,
       null as "zone",
       null as province,
       null as region,
       null as shop_type,
       null as mt_sub_channel,
       null as retail_environment,
       null as group_account,
       null as account,
       nvl(prod.franchise,'NA') as franchise,
       nvl(prod.brand,'NA') as brand,
       nvl(prod.variant,'NA') as variant,
       nvl(prod.product_group,'NA') as product_group,
       nvl(prod. group_jb,'NA') as group_jb,
       null as parameter_value,
       null as caln_yr_mo
    from
    (select cycle ,data_type,'MTI' as  sub_channel, dksh_mti*1000000 as tgt, jnj_code, description from itg_mds_vn_allchannel_siso_target_sku 
    union all 
    select cycle ,data_type,'MTD' as  sub_channel, mtd*1000000 as tgt, jnj_code, description from itg_mds_vn_allchannel_siso_target_sku)mt_sku_tgt
    inner join 
        (SELECT edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no
                FROM edw_vw_os_time_dim
                GROUP BY edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no)timedim
        on cast(timedim.mnth_id as numeric) = mt_sku_tgt.cycle
    left join prod_dim  prod
        on cast(mt_sku_tgt.jnj_code as varchar) = prod.sap_code
),

cte10 as
(
    select 'Sell-Out Actual' as data_type,
            'ECOM' as channel,
            'ECOM'  as sub_channel,
            timedim."year" as jj_year,
            time_dim.qrtr AS jj_qrtr,
            timedim.mnth_id AS jj_mnth_id,
            timedim.mnth_no AS jj_mnth_no,
            timedim.mnth_wk_no as jj_mnth_wk_no,
            time_dim.mnth_day as jj_mnth_day,
            null as  mapped_spk,
            null as dstrbtr_grp_cd,
            null as dstrbtr_name,
            null as sap_sold_to_code,
            gt_prod.sap_code as sap_matl_num,
            gt_prod.sap_matl_name,
            ecom.productid as dstrbtr_matl_num,
            ecom.product as dstrbtr_matl_name,
            ecom.barcode as bar_code,
            ecom.custcode as customer_code,
            ecom.customer as customer_name,
            to_date(ecom.invoice_date,'yyyymmdd') as invoice_date,
            null as salesman,
            null as salesman_name,
            null as si_gts_val,
            null as si_gts_excl_dm_val,
            null as si_nts_val,
            null as si_mnth_tgt_by_sku,
            ecom.gross_amount_wo_vat as so_net_trd_sls_loc,
            ecom.gross_amount_wo_vat*exch_rate.ex_rt as so_net_trd_sls_usd,
            null as so_mnth_tgt,
            null as so_avg_wk_tgt,
            null as so_mnth_tgt_by_sku,
            null as zone_manager_id,
            mds.kam  as zone_manager_name,
            ecom.zone,
            ecom.province,
            ecom.region,
            null as shop_type,
            cust.sub_channel as mt_sub_channel,
            cust.retail_environment,
            cust.group_account,
            cust.account,
            gt_prod.franchise,
            gt_prod.brand,
            gt_prod.variant,
            gt_prod.product_group,
            gt_prod.group_jb,
            null as parameter_value,
            null as caln_yr_mo
    from (SELECT itg_vn_mt_sellin_dksh.productid,
            itg_vn_mt_sellin_dksh.product,
            itg_vn_mt_sellin_dksh.custcode,
            itg_vn_mt_sellin_dksh.customer,
            itg_vn_mt_sellin_dksh.province,
            itg_vn_mt_sellin_dksh.region,
            itg_vn_mt_sellin_dksh.zone,
            itg_vn_mt_sellin_dksh.channel,
            itg_vn_mt_sellin_dksh.invoice_date,
            itg_vn_mt_sellin_dksh.gross_amount_wo_vat,
            itg_vn_mt_sellin_dksh.barcode
    from (SELECT dense_rank() OVER 
            (PARTITION BY itg_vn_mt_sellin_dksh.productid
            ,itg_vn_mt_sellin_dksh.custcode
            ,itg_vn_mt_sellin_dksh.billing_no
            ,itg_vn_mt_sellin_dksh.invoice_date
            ,itg_vn_mt_sellin_dksh.order_no ORDER BY itg_vn_mt_sellin_dksh.filename DESC) AS rnk,
             itg_vn_mt_sellin_dksh.productid,
             itg_vn_mt_sellin_dksh.product,
             itg_vn_mt_sellin_dksh.custcode,
             itg_vn_mt_sellin_dksh.customer,
             itg_vn_mt_sellin_dksh.province,
             itg_vn_mt_sellin_dksh.region,
             itg_vn_mt_sellin_dksh.zone,
             itg_vn_mt_sellin_dksh.channel,
             itg_vn_mt_sellin_dksh.invoice_date,
             itg_vn_mt_sellin_dksh.gross_amount_wo_vat,
             itg_vn_mt_sellin_dksh.barcode
      FROM itg_vn_mt_sellin_dksh
      WHERE itg_vn_mt_sellin_dksh.channel = 'ECOM') itg_vn_mt_sellin_dksh
WHERE itg_vn_mt_sellin_dksh.rnk = 1
UNION ALL
SELECT  itg_vn_mt_sellin_dksh_history.productid,
       itg_vn_mt_sellin_dksh_history.product,
       itg_vn_mt_sellin_dksh_history.custcode,
       itg_vn_mt_sellin_dksh_history.customer,
       itg_vn_mt_sellin_dksh_history.province,
       itg_vn_mt_sellin_dksh_history.region,
       itg_vn_mt_sellin_dksh_history.zone,
       itg_vn_mt_sellin_dksh_history.channel,
       itg_vn_mt_sellin_dksh_history.invoice_date,
       itg_vn_mt_sellin_dksh_history.net_amount_wo_vat,
       itg_vn_mt_sellin_dksh_history.barcode
FROM itg_vn_mt_sellin_dksh_history
WHERE itg_vn_mt_sellin_dksh_history.channel = 'ECOM' ) ecom
INNER JOIN edw_vw_os_time_dim timedim ON ecom.invoice_date = timedim.cal_date_id
inner join time_dim on to_date(ecom.invoice_date,'yyyymmdd') = time_dim.cal_date
left join edw_vw_vn_mt_dist_products mt_prod ON ecom.productid = mt_prod.code
left join prod_dim gt_prod
on gt_prod.sap_code = cast(mt_prod.jnj_sap_code as varchar)
LEFT JOIN edw_vw_vn_mt_dist_customers cust ON ecom.custcode = cust.code
LEFT JOIN (
        select distinct cast(mtd_code as varchar) as customer_cd,upper(sales_supervisor) as sales_supervisor,upper(sales_man)as ss,upper(kam) as kam 
        from  itg_vn_mt_customer_sales_organization where mti_code is null
        and active = 'Y'
        union all
        select distinct cast(mti_code as varchar) as customer_cd,upper(sales_supervisor) as sales_supervisor,upper(sales_man)as ss,upper(kam) as kam 
        from  itg_vn_mt_customer_sales_organization where mtd_code is null
        and active = 'Y'
    ) mds ON ecom.custcode::TEXT = mds.customer_cd::TEXT
LEFT JOIN (SELECT derived_table1."year" || derived_table1.mnth AS mnth_id,
                    derived_table1.ex_rt
             FROM (SELECT edw_crncy_exch_rates.fisc_yr_per,
                          substring(edw_crncy_exch_rates.fisc_yr_per::CHARACTER VARYING::TEXT,1,4) AS "year",
                          substring(edw_crncy_exch_rates.fisc_yr_per::CHARACTER VARYING::TEXT,6,2) AS mnth,
                          edw_crncy_exch_rates.ex_rt
                   FROM edw_crncy_exch_rates
                   WHERE edw_crncy_exch_rates.from_crncy::TEXT = 'VND'::CHARACTER VARYING::TEXT
                   AND   edw_crncy_exch_rates.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT) derived_table1) exch_rate
                    ON timedim.mnth_id::NUMERIC (18,0) = exch_rate.mnth_id::NUMERIC (18,0)
),

cte11 as 
(
    select 'Sell-Out Target' as data_type,
        'ECOM' as channel,
        'ECOM' as sub_channel,
        timedim."year" as jj_year,
        substring(timedim.qrtr,6,7) AS jj_qrtr,
        timedim.mnth_id AS jj_mnth_id,
        timedim.mnth_no AS jj_mnth_no,
        null as jj_mnth_wk_no,
        null as jj_mnth_day,
        null as mapped_spk,
        null as dstrbtr_grp_cd,
        null as dstrbtr_name,
        null as sap_sold_to_code,
        null as sap_matl_num,
        null as sap_matl_name,
        null as dstrbtr_matl_num,
        null as dstrbtr_matl_name,
        null as bar_code,
        null as customer_code,
        null as customer_name,
        null as invoice_date,
        null as salesman,
        null as salesman_name,
        null as si_gts_val,
        null as si_gts_excl_dm_val,
        null as si_nts_val,
        null as si_mnth_tgt_by_sku,
        null as so_net_trd_sls_loc,
        null as so_net_trd_sls_usd,
        ecom_tgt.target*1000000 as so_mnth_tgt,
        null as so_avg_wk_tgt,
        null as so_mnth_tgt_by_sku,
        null as zone_manager_id,
        null as zone_manager_name,
        null as "zone",
        null as province,
        null as region,
        null as shop_type,
        null as mt_sub_channel,
        null as retail_environment,
        null as group_account,
        upper(ecom_tgt.platform) as account,
        null as franchise,
        null as brand,
        null as variant,
        null as product_group,
        null as group_jb,
        null as parameter_value,
        null as caln_yr_mo
    from itg_mds_vn_ecom_target ecom_tgt
    inner join (SELECT edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no
             FROM edw_vw_os_time_dim
             GROUP BY edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no)timedim
    on cast(timedim.mnth_id as numeric) = ecom_tgt.cycle
),

cte12 as
(
    select case when ecom_tgt.data_type = 'Sell-In' then 'Sell-In Target'
          else 'Sell-Out Target'
          end  as data_type,
       'ECOM' as channel,
       'ECOM' as sub_channel,
       timedim."year" as jj_year,
       substring(timedim.qrtr,6,7) AS jj_qrtr,
       timedim.mnth_id AS jj_mnth_id,
       timedim.mnth_no AS jj_mnth_no,
       null as jj_mnth_wk_no,
       null as jj_mnth_day,
       null as mapped_spk,
       null as dstrbtr_grp_cd,
       null as dstrbtr_name,
       null as sap_sold_to_code,
       prod.sap_code as sap_matl_num,
       prod.sap_matl_name,
       cast(ecom_tgt.jnj_code as varchar) as dstrbtr_matl_num,
       ecom_tgt.description as dstrbtr_matl_name,
       null as bar_code,
       null as customer_code,
       null as customer_name,
       null as invoice_date,
       null as salesman,
       null as salesman_name,
       null as si_gts_val,
	   null as si_gts_excl_dm_val,
       null as si_nts_val,
       case when ecom_tgt.data_type = 'Sell-In' then ecom_tgt.dksh_ecom*1000000 else null end as si_mnth_tgt_by_sku,
       null as so_net_trd_sls_loc,
       null as so_net_trd_sls_usd,
       null as so_mnth_tgt,
       null as so_avg_wk_tgt,
       case when ecom_tgt.data_type = 'Sell-Out' then ecom_tgt.dksh_ecom*1000000 else null end as so_mnth_tgt_by_sku,
       null as zone_manager_id,
       null as zone_manager_name,
       null as "zone",
       null as province,
       null as region,
       null as shop_type,
       null as mt_sub_channel,
       null as retail_environment,
       null as group_account,
       null as account,
       nvl(prod.franchise,'NA') as franchise,
       nvl(prod.brand,'NA') as brand,
       nvl(prod.variant,'NA') as variant,
       nvl(prod.product_group,'NA') as product_group,
       nvl(prod. group_jb,'NA') as group_jb,
        null as parameter_value,
        null as caln_yr_mo
 from itg_mds_vn_allchannel_siso_target_sku ecom_tgt
    inner join 
    (SELECT edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no
                FROM edw_vw_os_time_dim
                GROUP BY edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no)timedim
    on cast(timedim.mnth_id as numeric) = ecom_tgt.cycle
    left join prod_dim prod
    on cast(ecom_tgt.jnj_code as varchar) = prod.sap_code
),

cte13 as 
(
    select 'Sell-Out Actual' as data_type,
		'OTC' as channel,
		ot.channel as sub_channel,
		timedim."year" as jj_year,
       time_dim.qrtr AS jj_qrtr,
       timedim.mnth_id AS jj_mnth_id,
       timedim.mnth_no AS jj_mnth_no,
       timedim.mnth_wk_no as jj_mnth_wk_no,
       time_dim.mnth_day as jj_mnth_day,
		null as mapped_spk,
		null as dstrbtr_grp_cd  ,
		'DKSH Gigamed' as dstrbtr_name ,
		null as sap_sold_to_code,
		ot.sap_matl_num,
		prod_dim.sap_matl_name,
		null as dstrbtr_matl_num,
		null as dstrbtr_matl_name,
		null as bar_code,
		ot.kunnr as customer_code,
		ot.name1 as customer_name,
		to_date(billingdate,'yyyymmdd') as invoice_date,
		null as salesman,
		null as salesman_name,
		null as si_gts_val,
		null as si_gts_excl_dm_val,
		null as si_nts_val,
		null as si_mnth_tgt_by_sku,
	    cast(ot.TT as numeric(38,5))  as so_net_trd_sls_loc,
		null as so_net_trd_sls_usd,
		null as so_mnth_tgt,
		null as so_avg_wk_tgt,
		null as so_mnth_tgt_by_sku,
		null as zone_manager_id,
		null as zone_manager_name,
		null as "zone",
		ot.province as province,
		ot.region as region,
		null as shop_type,
		null as mt_sub_channel,
		ot.custgroup as retail_environment,
		'N/A' as group_account,
		'N/A' as account,
		 prod_dim.franchise,
       prod_dim.brand,
       prod_dim.variant,
       prod_dim.product_group,
       prod_dim.group_jb,
        null as parameter_value,
        null as caln_yr_mo
	from itg_vn_oneview_otc ot	
    inner join edw_vw_os_time_dim timedim ON ot.billingdate = timedim.cal_date_id
    inner join time_dim on to_date(billingdate,'yyyymmdd') = time_dim.cal_date          
	left join prod_dim on prod_dim.sap_code = ot.sap_matl_num
	where(TT<>0)
),

cte14 as 
(
    select case when otc_tgt.data_type = 'Sell-In' then 'Sell-In Target'
          else 'Sell-Out Target'
          end  as data_type,
       'OTC' channel,
       'OTC' as sub_channel,
       timedim."year" as jj_year,
       substring(timedim.qrtr,6,7) AS jj_qrtr,
       timedim.mnth_id AS jj_mnth_id,
       timedim.mnth_no AS jj_mnth_no,
       null as jj_mnth_wk_no,
       null as jj_mnth_day,
       null as mapped_spk,
       null as dstrbtr_grp_cd,
       null as dstrbtr_name,
       null as sap_sold_to_code,
       prod.sap_code as sap_matl_num,
       prod.sap_matl_name,
       cast(otc_tgt.jnj_code as varchar) as dstrbtr_matl_num,
       otc_tgt.description as dstrbtr_matl_name,
       null as bar_code,
       null as customer_code,
       null as customer_name,
       null as invoice_date,
       null as salesman,
       null as salesman_name,
       null as si_gts_val,
	   null as si_gts_excl_dm_val,
       null as si_nts_val,
       case when otc_tgt.data_type = 'Sell-In' then cast(otc_tgt.otc as float)*1000000 else null end as si_mnth_tgt_by_sku,
       null as so_net_trd_sls_loc,
       null as so_net_trd_sls_usd,
       null as so_mnth_tgt,
       null as so_avg_wk_tgt,
       case when otc_tgt.data_type = 'Sell-Out' then cast(otc_tgt.otc as float)*1000000 else null end as so_mnth_tgt_by_sku,
       null as zone_manager_id,
       null as zone_manager_name,
       null as "zone",
       null as province,
       null as region,
       null as shop_type,
       null as mt_sub_channel,
       null as retail_environment,
       null as group_account,
       null as account,
       nvl(prod.franchise,'NA') as franchise,
       nvl(prod.brand,'NA') as brand,
       nvl(prod.variant,'NA') as variant,
       nvl(prod.product_group,'NA') as product_group,
       nvl(prod.group_jb,'NA') as group_jb,
        null as parameter_value,
        null as caln_yr_mo
 from itg_mds_vn_allchannel_siso_target_sku otc_tgt
    inner join 
    (SELECT edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no
                FROM edw_vw_os_time_dim
                GROUP BY edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.mnth_no)timedim
    on cast(timedim.mnth_id as numeric) = otc_tgt.cycle
    left join prod_dim prod
    on cast(otc_tgt.jnj_code as varchar) = prod.sap_code
)


-- select * from cte1_filtered
-- union all
-- select * from cte2_filtered
-- union all
-- select * from cte3
-- union all
-- select * from cte4
-- union all
-- select * from cte5
-- union all
select 'cte6' nm,* from cte6
union all
select 'cte7' nm,* from cte7
union all
select 'cte8' nm,* from cte8
union all
select 'cte9' nm,* from cte9
union all
-- select * from cte10
-- union all
select 'cte11' nm,* from cte11
union all
select 'cte12' nm,* from cte12
-- union all
-- select * from cte13
union all
select 'cte14' nm,* from cte14