with itg_vn_dms_product_dim as (
    select * from {{ ref(vnmitg_integration__itg_vn_dms_product_dim) }}
),
edw_vw_vn_mt_dist_products as (
    select * from {{ ref(vnmedw_integration__edw_vw_vn_mt_dist_products) }}
),
itg_mds_vn_allchannel_siso_target_sku as (
    select * from {{ ref(vnmitg_integration__itg_mds_vn_allchannel_siso_target_sku) }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as (
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
itg_query_parameters as (
    select * from {{ source('vnmitg_integration', 'itg_query_parameters') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
v_edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),

prod_dim as 
(
    select 
        prod.sap_code as sap_code,
        rpd.matl_desc as sap_matl_name,
        rpd.pka_franchise_desc as franchise,
        gcph.gcph_brand + ' / ' + gcph.gcph_segment as brand,
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
    SELECT year,
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
    from os_edw.edw_vw_vn_billing_fact 
    left join os_edw.edw_vw_os_time_dim timedim ON bill_dt = timedim.cal_date
    where cntry_key = 'VN' AND sold_to = '133806'
      AND bill_type in ('ZF2V', 'S1') AND bill_qty_pc <> 0 
    group by 1 
    having sum(case when ship_to = '624097' then grs_trd_sls else 0 end)>0
)
	  	  
-----------------------------Sell-In Actual--------------------------------------
	   
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
      prod_dim.group_jb
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
  WHERE UPPER(copa.ctry_key) in ('VN')
  AND
  CASE
	WHEN qry_param.parameter_value = 'MTI' 
	THEN jj_mnth_id in (select distinct CAST(caln_yr_mo AS varchar(23)) as jj_mnth_id from edw_copa_trans_fact where jj_mnth_id not between '202105' and '202209')
	ELSE jj_mnth_id = CAST(caln_yr_mo AS varchar(23))
  END
  AND (si_gts_val IS NOT NULL 
  	   OR si_gts_excl_dm_val IS NOT NULL
  	   OR si_nts_val IS NOT NULL)