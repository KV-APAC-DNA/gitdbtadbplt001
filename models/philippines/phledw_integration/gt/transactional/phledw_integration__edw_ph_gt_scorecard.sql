with edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_ph_pos_pricelist as (
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_ph_dms_sellout_sales_fact as (
select * from {{ ref('phlitg_integration__itg_ph_dms_sellout_sales_fact') }}
),
itg_mds_ph_distributor_product as (
select * from {{ ref('phlitg_integration__itg_mds_ph_distributor_product') }}
),
edw_ph_sellout_analysis as (
select * from {{ ref('phledw_integration__edw_ph_sellout_analysis') }}
),
itg_mds_ph_ref_distributors as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ref_distributors') }}
),
edw_ph_siso_analysis as (
select * from {{ ref('phledw_integration__edw_ph_siso_analysis') }}
),
itg_query_parameters as (
select * from DEV_DNA_CORE.phlitg_INTEGRATION.ITG_QUERY_PARAMETERS
),
edw_vw_ph_dstrbtr_customer_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_customer_dim') }}
),
edw_mv_ph_customer_dim as (
select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_ph_material_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
itg_ph_cpg_calls as (
select * from {{ ref('phlitg_integration__itg_ph_cpg_calls') }}
),
itg_mds_ph_lav_product as (
select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
itg_mds_ph_msl_hdr as (
select * from {{ ref('phlitg_integration__itg_mds_ph_msl_hdr') }}
),
itg_mds_ph_msl_dtls as (
select * from {{ ref('phlitg_integration__itg_mds_ph_msl_dtls') }}
),
time_dim AS
(
  SELECT *,
         CAST(LAG(mnth_id,2) OVER (ORDER BY mnth_id) AS INTEGER) AS L3M
  FROM (SELECT "year",
               qrtr,
               CAST(mnth_id AS INTEGER) mnth_id,
               mnth_no,
               MIN(cal_date) AS start_date,
               MAX(cal_date) AS end_date
        FROM edw_vw_os_time_dim
        GROUP BY "year",
                 qrtr,
                 mnth_id,
                 mnth_no)
),
listprice AS
(
  SELECT b.mnth_id,
         b.item_cd,
         a.lst_price_unit
  FROM (SELECT *
        FROM itg_mds_ph_pos_pricelist
        WHERE UPPER(active) = 'Y') a
    INNER JOIN (SELECT mnth_id,
                       item_cd,
                       MAX(jj_mnth_id) AS pl_mnth_id
                FROM (SELECT tm.mnth_id,
                             so.*,
                             dist.sap_item_cd,
                             pl.item_cd,
                             pl.jj_mnth_id
                      FROM itg_ph_dms_sellout_sales_fact so
                        LEFT JOIN edw_vw_os_time_dim tm ON tm.cal_date = so.invoice_dt
                        LEFT JOIN itg_mds_ph_distributor_product dist
                               ON so.dstrbtr_grp_cd = dist.dstrbtr_grp_cd
                              AND so.dstrbtr_prod_id = dist.dstrbtr_item_cd
                              AND UPPER (dist.active) = 'Y'
                        LEFT JOIN itg_mds_ph_pos_pricelist pl
                               ON item_cd = (CASE WHEN so.dstrbtr_prod_id <> dist.sap_item_cd THEN dist.sap_item_cd ELSE so.dstrbtr_prod_id END)
                              AND pl.jj_mnth_id <= tm.mnth_id
                              AND UPPER (pl.active) = 'Y')
                GROUP BY mnth_id,
                         item_cd) b
            ON a.item_cd = b.item_cd
           AND a.jj_mnth_id = b.pl_mnth_id
),
MSLProducts AS (
SELECT *
FROM (
SELECT DISTINCT b.sku_code, b.sku_name, a.csg_code, a.from_salescycle, a.to_salescycle, 'y' tagging,c.mnth_id
FROM itg_mds_ph_msl_hdr a LEFT JOIN itg_mds_ph_msl_dtls b
on a.msl_hdr_code = b.msl_hdr_code
LEFT join edw_vw_os_time_dim c
on c.mnth_id BETWEEN replace(substring(a.from_salescycle,1,7),'-','') and replace(substring(a.to_salescycle,1,7),'-','')
where c.mnth_id is not null and c.mnth_id <> ''
)
),
MSLSellout AS (
SELECT DISTINCT 
                   jj_year,
                   jj_qrtr,
                   jj_mnth_id,
                   jj_mnth_no,
                   cntry_nm,
                   account_grp,
                   sls_grp_desc,
                   sub_chnl_cd,
                   dstrbtr_grp_cd,
                   dstrbtr_cust_cd,
                   chnl_desc,
                   global_prod_franchise,
                   global_prod_brand,
                   global_prod_segment,
                   rka_cd,
                   rka_nm,
                   sku
            FROM EDW_PH_SELLOUT_ANALYSIS abc            
            where rka_cd is null or (rka_cd is not null and (rka_cd = ' ' or rka_cd not in('RKA008', 'RKA032')))
      AND jj_grs_trd_sls is not null and jj_grs_trd_sls > 0
      AND jj_year >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*547.5 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))),	
CntMSLProducts AS (
   SELECT mnth_id, csg_code, COUNT(sku_code) as cnt_msl
  FROM (SELECT DISTINCT
          b.sku_code,
          b.sku_name,
          a.csg_code,
          a.from_salescycle,
          a.to_salescycle,
          'y' tagging,
          c.mnth_id
        FROM itg_mds_ph_msl_hdr a LEFT JOIN itg_mds_ph_msl_dtls b
          ON a.msl_hdr_code = b.msl_hdr_code
        LEFT join edw_vw_os_time_dim c
          ON c.mnth_id BETWEEN replace(substring(a.from_salescycle,1,7),'-','') and replace(substring(a.to_salescycle,1,7),'-','')
        WHERE
          c.mnth_id is not null
          AND c.mnth_id <> ''
          )
          GROUP BY
        mnth_id, csg_code
          
),
CntSelloutMSL as
    (
      SELECT jj_year,
    jj_qrtr,
    jj_mnth_id,
    jj_mnth_no,
    cntry_nm,
    account_grp,
    sls_grp_desc,
    sub_chnl_cd,
    dstrbtr_grp_cd,
    dstrbtr_cust_cd,
    chnl_desc,
    global_prod_franchise,
    global_prod_brand,
    global_prod_segment,
    rka_cd,
    rka_nm,
     COUNT(SKU) AS cnt_sku
      FROM (SELECT DISTINCT a.jj_year,
    a.jj_qrtr,
    a.jj_mnth_id,
    a.jj_mnth_no,
    a.cntry_nm,
    a.account_grp,
    a.sls_grp_desc,
    a.sub_chnl_cd,
    a.dstrbtr_grp_cd,
    a.dstrbtr_cust_cd,
    a.chnl_desc,
    a.global_prod_franchise,
    a.global_prod_brand,
    a.global_prod_segment,
    a.rka_cd,
    a.rka_nm,
       a.sku
            FROM MSLSellout a LEFT JOIN MSLProducts b
            ON
              a.jj_mnth_id = b.mnth_id
              AND a.sku = b.sku_code
              AND a.sub_chnl_cd = b.csg_code
            WHERE
              
               b.tagging = 'y')
      GROUP BY
        jj_year,
    jj_qrtr,
    jj_mnth_id,
    jj_mnth_no,
    cntry_nm,
    account_grp,
    sls_grp_desc,
    sub_chnl_cd,
    dstrbtr_grp_cd,
    dstrbtr_cust_cd,
    chnl_desc,
    global_prod_franchise,
    global_prod_brand,
    global_prod_segment,
    rka_cd,
    rka_nm
    ),
transformed as (   
SELECT *
FROM (SELECT 'Sellout' AS Identifier,
             jj_year,
             jj_qrtr,
             a.jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,
             NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             dstrbtr_cust_cd AS trnsfrm_cust_id,
			      NULL AS invoice_dt,
             chnl_desc AS chnl_desc,
             global_prod_franchise AS franchise,
             global_prod_brand AS brand,
             global_prod_segment AS segment,
             rka_cd,
             rka_nm,
             SUM(JJ_GRS_TRD_SLS + JJ_RET_VAL - TRD_DISCNT) AS so_gts_val,
             0 AS si_gts_val,
             SUM(JJ_RET_VAL) AS ret_val,
             0 AS unserved,
             SUM(CASE WHEN UPPER(is_promo) = 'Y' THEN (JJ_GRS_TRD_SLS + JJ_RET_VAL - TRD_DISCNT) ELSE 0 END) promo_national,
             0 AS promo_local,
             SUM(CASE WHEN UPPER(is_npi) = 'Y' THEN (JJ_GRS_TRD_SLS + JJ_RET_VAL - TRD_DISCNT) ELSE 0 END) npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             SUM((JJ_GRS_TRD_SLS + JJ_RET_VAL - TRD_DISCNT) / c.max_week) AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT * FROM edw_ph_sellout_analysis) a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN (SELECT MAX(jj_mnth_wk_no) AS max_week,
                          jj_mnth_id
                   FROM edw_ph_siso_analysis
                   GROUP BY jj_mnth_id) c ON a.jj_mnth_id = c.jj_mnth_id
      WHERE jj_year >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               a.jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               rka_cd,
               rka_nm
      UNION ALL
      SELECT 'Sellin-Inventory' AS Identifier,
             jj_year,
             jj_qrtr,
             a.jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,
         NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             NULL AS trnsfrm_cust_id,
			 NULL AS invoice_dt,
             si_chnl_desc AS chnl_desc,
             global_prod_franchise AS franchise,
             global_prod_brand AS brand,
             global_prod_segment AS segment,
             NULL AS rka_cd,
             NULL AS rka_nm,
             0 AS so_gts_val,
             SUM(SI_GTS_LESS_RTN_VAL) AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             SUM(CASE WHEN jj_mnth_wk_no = c.max_week THEN end_stock_val ELSE NULL END) AS inventory,
             SUM(SI_GTS_LESS_RTN_VAL / c.max_week) AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM edw_ph_siso_analysis a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN (SELECT MAX(jj_mnth_wk_no) AS max_week,
                          jj_mnth_id
                   FROM edw_ph_siso_analysis
                   GROUP BY jj_mnth_id) c ON a.jj_mnth_id = c.jj_mnth_id
      WHERE jj_year >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               a.jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               rka_cd,
               rka_nm
      UNION ALL
      SELECT 'Unserved' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             e.mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             d.rpt_grp_6_desc AS account_grp,
             d.rpt_grp_2_desc AS sls_grp_desc,
             NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             trnsfrm_cust_id,
			 NULL AS invoice_dt,
             a.chnl_desc,
             h.gph_prod_frnchse AS franchise,
             h.gph_prod_brnd AS brand,
             h.gph_prod_sgmnt AS segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             SUM((order_qty - qty)*g.lst_price_unit) AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			--- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT sellout.*,
                   customer.chnl_desc,
                   customer.sls_dstrct_cd,
                   customer.sls_dstrct_nm,
                   dist.sap_item_cd,
                   (CASE WHEN sellout.dstrbtr_prod_id <> dist.sap_item_cd THEN dist.sap_item_cd ELSE sellout.dstrbtr_prod_id END) AS finsku
            FROM (SELECT *
                  FROM ITG_PH_DMS_SELLOUT_SALES_FACT
                  WHERE order_qty > qty
                  AND   order_qty > 0
                  AND   order_qty IS NOT NULL
                  AND   TO_CHAR(invoice_dt,'yyyy') >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) sellout
              LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                     ON sellout.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                    AND sellout.trnsfrm_cust_id = customer.cust_cd
                    AND UPPER (customer.cntry_cd) = 'PH'
              LEFT JOIN itg_mds_ph_distributor_product dist
                     ON sellout.dstrbtr_grp_cd = dist.dstrbtr_grp_cd
                    AND sellout.dstrbtr_prod_id = dist.dstrbtr_item_cd
                    AND UPPER (dist.active) = 'Y') a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm,
                          primary_sold_to AS sap_soldto_code
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id
        LEFT JOIN edw_vw_os_time_dim e ON a.invoice_dt = e.cal_date
        LEFT JOIN listprice g
               ON g.item_cd = a.finsku
              AND e.mnth_id = g.mnth_id
        LEFT JOIN edw_vw_os_material_dim h
               ON LTRIM (a.sap_item_cd,0) = LTRIM (h.sap_matl_num,0)
              AND UPPER (h.cntry_key) = 'PH'
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
              sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               sls_dstrct_cd,
               sls_dstrct_nm
      UNION ALL
      SELECT 'Promo_local' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             d.rpt_grp_6_desc AS account_grp,
             d.rpt_grp_2_desc AS sls_grp_desc,
             NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             a.cust_cd AS trnsfrm_cust_id,
			 NULL AS invoice_dt,
             a.chnl_desc,
             h.gph_prod_frnchse AS franchise,
             h.gph_prod_brnd AS brand,
             h.gph_prod_sgmnt AS segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             SUM(gts) AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			--- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT sellout.*,
                   sellout.gts_val - sellout.dscnt AS gts,
                   dist.sap_item_cd,
                   customer.chnl_desc,
                   customer.cust_cd,
                   customer.sls_dstrct_cd,
                   customer.sls_dstrct_nm
            FROM (SELECT *
                  FROM ITG_MDS_PH_DISTRIBUTOR_PRODUCT
                  WHERE UPPER(active) = 'Y') dist
              LEFT JOIN (SELECT *
                         FROM ITG_PH_DMS_SELLOUT_SALES_FACT
                         WHERE gts_val IS NOT NULL
                         AND   gts_val <> 0
                         AND   TO_CHAR(invoice_dt,'yyyy') >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) sellout
                     ON dist.dstrbtr_grp_cd = sellout.dstrbtr_grp_cd
                    AND dist.dstrbtr_item_cd = sellout.dstrbtr_prod_id
              LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                     ON sellout.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                    AND sellout.trnsfrm_cust_id = customer.cust_cd
                    AND UPPER (customer.cntry_cd) = 'PH') a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm,
                          primary_sold_to AS sap_soldto_code
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id
        LEFT JOIN edw_vw_os_time_dim e ON a.invoice_dt = e.cal_date
        LEFT JOIN edw_vw_os_material_dim h
               ON LTRIM (a.sap_item_cd,0) = LTRIM (h.sap_matl_num,0)
              AND UPPER (h.cntry_key) = 'PH'
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               a.cust_cd,
               chnl_desc,
               franchise,
               brand,
               segment,
               sls_dstrct_cd,
               sls_dstrct_nm
      UNION ALL
      SELECT 'CPG' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             d.rpt_grp_6_desc AS account_grp,
             d.rpt_grp_2_desc AS sls_grp_desc,
             NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             trnsfrm_cust_id,
			 NULL AS invoice_dt,
             a.chnl_desc,
             NULL AS franchise,
             NULL AS brand,
             NULL AS segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             SUM(planned) AS planned_visit,
             SUM(actual) AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT visit.*,
                   customer.chnl_desc,
                   customer.sls_dstrct_cd,
                   customer.sls_dstrct_nm
            FROM (SELECT planned_visit,
                         dstrbtr_grp_cd,
                         trnsfrm_cust_id,
                         1 AS planned,
                         CASE
                           WHEN actual_visit IS NOT NULL THEN 1
                           ELSE 0
                         END AS actual
                  FROM ITG_PH_CPG_CALLS
                  WHERE approved_flag IS NOT NULL
                  AND   approved_flag = 1
                  AND   planned_visit IS NOT NULL
                  AND   year(planned_visit) >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) visit
              LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                     ON visit.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                    AND visit.trnsfrm_cust_id = customer.cust_cd
                    AND UPPER (customer.cntry_cd) = 'PH') a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm,
                          primary_sold_to AS sap_soldto_code
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id
        LEFT JOIN edw_vw_os_time_dim e ON a.planned_visit = e.cal_date
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               sls_dstrct_cd,
               sls_dstrct_nm
      UNION ALL
      SELECT 'Lines Sold-UBA' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             account_grp,
             sls_grp_desc,
         NULL AS sub_chnl_cd,
             dstrbtr_grp_cd,
             dstrbtr_grp_nm,
             trnsfrm_cust_id,
			 NULL AS invoice_dt,
             chnl_desc,
             franchise,
             brand,
             segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             lines AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT "year",
                   qrtr,
                   mnth_id,
                   mnth_no,
                   account_grp,
                   sls_grp_desc,
                   dstrbtr_grp_cd,
                   dstrbtr_grp_nm,
                   trnsfrm_cust_id,
                   chnl_desc,
                   franchise,
                   brand,
                   segment,
                   sls_dstrct_cd,
                   sls_dstrct_nm,
                   COUNT(*) AS lines
            FROM (SELECT "year",
                         qrtr,
                         mnth_id,
                         mnth_no,
                         d.rpt_grp_6_desc AS account_grp,
                         d.rpt_grp_2_desc AS sls_grp_desc,
                         sellout.dstrbtr_grp_cd,
                         b.dstrbtr_grp_nm,
                         sellout.trnsfrm_cust_id,
                         customer.chnl_desc,
                         mat.gph_prod_frnchse AS franchise,
                         mat.gph_prod_brnd AS brand,
                         mat.gph_prod_sgmnt AS segment,
                         customer.sls_dstrct_cd,
                         customer.sls_dstrct_nm
                  FROM (SELECT *
                        FROM ITG_PH_DMS_SELLOUT_SALES_FACT
                        WHERE gts_val IS NOT NULL
                        AND   gts_val > 0
                        AND   TO_CHAR(invoice_dt,'yyyy') >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) sellout
                    LEFT JOIN edw_vw_os_time_dim TIME ON sellout.invoice_dt = time.cal_date
                    LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                           ON sellout.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                          AND sellout.trnsfrm_cust_id = customer.cust_cd
                          AND UPPER (customer.cntry_cd) = 'PH'
                    LEFT JOIN (SELECT dstrbtr_grp_cd,
                                      dstrbtr_grp_nm,
                                      primary_sold_to AS sap_soldto_code
                               FROM itg_mds_ph_ref_distributors
                               WHERE UPPER(active) = 'Y') b ON sellout.dstrbtr_grp_cd = b.dstrbtr_grp_cd
                    LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id
                    LEFT JOIN itg_mds_ph_distributor_product dist
                           ON sellout.dstrbtr_grp_cd = dist.dstrbtr_grp_cd
                          AND sellout.dstrbtr_prod_id = dist.dstrbtr_item_cd
                          AND UPPER (dist.active) = 'Y'
                    LEFT JOIN edw_vw_os_material_dim mat
                           ON LTRIM (dist.sap_item_cd,0) = LTRIM (mat.sap_matl_num,0)
                          AND UPPER (mat.cntry_key) = 'PH')
            GROUP BY "year",
                     qrtr,
                     mnth_id,
                     mnth_no,
                     account_grp,
                     sls_grp_desc,                   
                     dstrbtr_grp_cd,
                     dstrbtr_grp_nm,
                     trnsfrm_cust_id,
                     chnl_desc,
                     franchise,
                     brand,
                     segment,
                     sls_dstrct_cd,
                     sls_dstrct_nm)
	  UNION ALL
	  SELECT 'Effective' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             account_grp,
             sls_grp_desc,
              NULL AS sub_chnl_cd,
             dstrbtr_grp_cd,
             dstrbtr_grp_nm,
             trnsfrm_cust_id,
             invoice_dt,
             chnl_desc,
             franchise,
             brand,
             segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             lines AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT "year",
                   qrtr,
                   mnth_id,
                   mnth_no,
                   account_grp,
                   sls_grp_desc,
                   dstrbtr_grp_cd,
                   dstrbtr_grp_nm,
                   trnsfrm_cust_id,
                   invoice_dt,
                   chnl_desc,
                   franchise,
                   brand,
                   segment,
                   sls_dstrct_cd,
                   sls_dstrct_nm,
                   COUNT(*) AS lines
            FROM (SELECT "year",
                         qrtr,
                         mnth_id,
                         mnth_no,
                         d.rpt_grp_6_desc AS account_grp,
                         d.rpt_grp_2_desc AS sls_grp_desc,
                         sellout.dstrbtr_grp_cd,
                         b.dstrbtr_grp_nm,
                         sellout.trnsfrm_cust_id,
                         sellout.invoice_dt,
                         customer.chnl_desc,
                         mat.gph_prod_frnchse AS franchise,
                         mat.gph_prod_brnd AS brand,
                         mat.gph_prod_sgmnt AS segment,
                         customer.sls_dstrct_cd,
                         customer.sls_dstrct_nm
                  FROM (SELECT *
                        FROM ITG_PH_DMS_SELLOUT_SALES_FACT
                        WHERE gts_val IS NOT NULL
                        AND   gts_val > 0
                        AND   TO_CHAR(invoice_dt,'yyyy') >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) sellout
                    LEFT JOIN edw_vw_os_time_dim TIME ON sellout.invoice_dt = time.cal_date
                    LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                           ON sellout.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                          AND sellout.trnsfrm_cust_id = customer.cust_cd
                          AND UPPER (customer.cntry_cd) = 'PH'
                    LEFT JOIN (SELECT dstrbtr_grp_cd,
                                      dstrbtr_grp_nm,
                                      primary_sold_to AS sap_soldto_code
                               FROM itg_mds_ph_ref_distributors
                               WHERE UPPER(active) = 'Y') b ON sellout.dstrbtr_grp_cd = b.dstrbtr_grp_cd
                    LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id
                    LEFT JOIN itg_mds_ph_distributor_product dist
                           ON sellout.dstrbtr_grp_cd = dist.dstrbtr_grp_cd
                          AND sellout.dstrbtr_prod_id = dist.dstrbtr_item_cd
                          AND UPPER (dist.active) = 'Y'
                    LEFT JOIN edw_vw_os_material_dim mat
                           ON LTRIM (dist.sap_item_cd,0) = LTRIM (mat.sap_matl_num,0)
                          AND UPPER (mat.cntry_key) = 'PH')
            GROUP BY "year",
                     qrtr,
                     mnth_id,
                     mnth_no,
                     account_grp,
                     sls_grp_desc,                  
                     dstrbtr_grp_cd,
                     dstrbtr_grp_nm,
                     trnsfrm_cust_id,
                     invoice_dt,
                     chnl_desc,
                     franchise,
                     brand,
                     segment,
                     sls_dstrct_cd,
                     sls_dstrct_nm)
      UNION ALL
      SELECT 'Active Buying Accounts' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             account_grp,
             sls_grp_desc,
              NULL AS sub_chnl_cd,
             dstrbtr_grp_cd,
             dstrbtr_grp_nm,
             trnsfrm_cust_id,
			 NULL AS invoice_dt,
             chnl_desc,
             NULL AS franchise,
             NULL AS brand,
             NULL AS segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             Census AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT "year",
                   qrtr,
                   CAST(mnth_id AS VARCHAR) mnth_id,
                   mnth_no,
                   account_grp,
                   sls_grp_desc,
                   dstrbtr_grp_cd,
                   dstrbtr_grp_nm,
                   trnsfrm_cust_id,
                   chnl_desc,
                   sls_dstrct_cd,
                   sls_dstrct_nm,
                   COUNT(*) AS Census
            FROM (SELECT DISTINCT "year",
                         qrtr,
                         mnth_id,
                         mnth_no,
                         d.rpt_grp_6_desc AS account_grp,
                         d.rpt_grp_2_desc AS sls_grp_desc,
                         sellout.dstrbtr_grp_cd,
                         b.dstrbtr_grp_nm,
                         sellout.trnsfrm_cust_id,
                         customer.chnl_desc,
                         customer.sls_dstrct_cd,
                         customer.sls_dstrct_nm
                  FROM (SELECT *
                        FROM ITG_PH_DMS_SELLOUT_SALES_FACT
                        WHERE gts_val IS NOT NULL
                        AND   gts_val > 0
                        AND   TO_CHAR(invoice_dt,'yyyy') >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) sellout
                    LEFT JOIN (SELECT a."year",
                                      a.qrtr,
                                      a.mnth_id,
                                      a.mnth_no,
                                      b.start_date,
                                      a.end_date
                               FROM time_dim a
                                 LEFT JOIN time_dim b ON a.l3m = b.mnth_id) TIME
                           ON sellout.invoice_dt BETWEEN time.start_date
                          AND time.end_date
                    LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                           ON sellout.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                          AND sellout.trnsfrm_cust_id = customer.cust_cd
                          AND UPPER (customer.cntry_cd) = 'PH'
                    LEFT JOIN (SELECT dstrbtr_grp_cd,
                                      dstrbtr_grp_nm,
                                      primary_sold_to AS sap_soldto_code
                               FROM itg_mds_ph_ref_distributors
                               WHERE UPPER(active) = 'Y') b ON sellout.dstrbtr_grp_cd = b.dstrbtr_grp_cd
                    LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id)
            GROUP BY "year",
                     qrtr,
                     mnth_id,
                     mnth_no,
                     account_grp,
                     sls_grp_desc,
                     dstrbtr_grp_cd,
                     dstrbtr_grp_nm,
                     trnsfrm_cust_id,
                     chnl_desc,
                     sls_dstrct_cd,
                     sls_dstrct_nm)
      UNION ALL
      SELECT 'Coverage' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             c.rpt_grp_6_desc AS account_grp,
             c.rpt_grp_2_desc AS sls_grp_desc,
              NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             a.trnsfrm_cust_id,
			 NULL AS invoice_dt,
             a.chnl_desc,
             NULL AS franchise,
             NULL AS brand,
             NULL AS segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT covered.*,
                   customer.chnl_desc,
                   customer.sls_dstrct_cd,
                   customer.sls_dstrct_nm
            FROM (SELECT DISTINCT "year",
                         qrtr,
                         mnth_id,
                         mnth_no,
                         dstrbtr_grp_cd,
                         dstrbtr_cust_id,
                         trnsfrm_cust_id
                  FROM ITG_PH_CPG_CALLS call
                    LEFT JOIN edw_vw_os_time_dim tm ON call.actual_visit = tm.cal_date
                  WHERE actual_visit IS NOT NULL
                  AND   TO_CHAR(actual_visit,'yyyy') >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) covered
              LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                     ON covered.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                    AND covered.trnsfrm_cust_id = customer.cust_cd
                    AND UPPER (customer.cntry_cd) = 'PH'
                   ) a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm,
                          primary_sold_to AS sap_soldto_code
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN edw_mv_ph_customer_dim c ON b.sap_soldto_code = c.cust_id
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               a.trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               sls_dstrct_cd,
               sls_dstrct_nm
      UNION ALL
      SELECT 'CSL' AS Identifier,
             "year" AS jj_year,
             qrtr AS jj_qrtr,
             mnth_id AS jj_mnth_id,
             mnth_no AS jj_mnth_no,
             'Philippines' AS cntry_nm,
             d.rpt_grp_6_desc AS account_grp,
             d.rpt_grp_2_desc AS sls_grp_desc,
              NULL AS sub_chnl_cd,
             a.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             trnsfrm_cust_id,
			 NULL AS invoice_dt,
             a.chnl_desc,
             f.gph_prod_frnchse AS franchise,
             f.gph_prod_brnd AS brand,
             f.gph_prod_sgmnt AS segment,
             sls_dstrct_cd AS rka_cd,
             sls_dstrct_nm AS rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             SUM(ontime) AS ontime,
             SUM(infull) AS infull,
             SUM(otif) AS otif,
             SUM(order_qty) AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
			 0 AS tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			--- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT sellout.*,
                   customer.chnl_desc,
                   customer.sls_dstrct_cd,
                   customer.sls_dstrct_nm,
                   dist.sap_item_cd
            FROM (SELECT *,
                         order_dt +3 AS Exp_Dlvy,
                         CASE
                           WHEN UPPER(order_status) = 'COMPLETED' AND order_qty = qty THEN qty
                           ELSE 0
                         END AS INFULL,
                         CASE
                           WHEN UPPER(order_status) = 'COMPLETED' AND invoice_dt <= order_dt +3 THEN qty
                           ELSE 0
                         END AS ONTIME,
                         CASE
                           WHEN UPPER(order_status) = 'COMPLETED' AND order_qty = qty AND invoice_dt <= order_dt +3 THEN qty
                           ELSE 0
                         END AS OTIF
                  FROM ITG_PH_DMS_SELLOUT_SALES_FACT
                  WHERE invoice_dt IS NOT NULL
                  AND   order_qty > 0
                  AND   TO_CHAR(invoice_dt,'yyyy') >=(SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))) sellout
              LEFT JOIN edw_vw_os_dstrbtr_customer_dim customer
                     ON sellout.dstrbtr_grp_cd = customer.dstrbtr_grp_cd
                    AND sellout.trnsfrm_cust_id = customer.cust_cd
                    AND UPPER (customer.cntry_cd) = 'PH'
              LEFT JOIN itg_mds_ph_distributor_product dist
                     ON sellout.dstrbtr_grp_cd = dist.dstrbtr_grp_cd
                    AND sellout.dstrbtr_prod_id = dist.dstrbtr_item_cd
                    AND UPPER (dist.active) = 'Y') a
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm,
                          primary_sold_to AS sap_soldto_code
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON a.dstrbtr_grp_cd = b.dstrbtr_grp_cd
        LEFT JOIN edw_mv_ph_customer_dim d ON b.sap_soldto_code = d.cust_id
        LEFT JOIN edw_vw_os_time_dim e ON a.invoice_dt = e.cal_date
        LEFT JOIN edw_vw_os_material_dim f
               ON LTRIM (a.sap_item_cd,0) = LTRIM (f.sap_matl_num,0)
              AND UPPER (f.cntry_key) = 'PH'
      GROUP BY Identifier,
               jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               a.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               sls_dstrct_cd,
               sls_dstrct_nm
	  UNION ALL
	  SELECT 'TDP' AS Identifier,
             jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,
              NULL AS sub_chnl_cd,
             base.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             dstrbtr_cust_cd AS trnsfrm_cust_id,
			 NULL AS invoice_dt,
             chnl_desc AS chnl_desc,
             global_prod_franchise AS franchise,
             global_prod_brand AS brand,
             global_prod_segment AS segment,
             rka_cd,
             rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
             COUNT(*) tdp,
			 0 AS npi_tdp,
			 0 As msl_tdp,
			---- 0 As cnt_sku,
       0 As cnt_msl
      FROM (SELECT DISTINCT jj_year,
                   jj_qrtr,
                   jj_mnth_id,
                   jj_mnth_no,
                   cntry_nm,
                   account_grp,
                   sls_grp_desc,
                   dstrbtr_grp_cd,
                   dstrbtr_cust_cd,
                   chnl_desc,
                   global_prod_franchise,
                   global_prod_brand,
                   global_prod_segment,
                   rka_cd,
                   rka_nm,
                   sku
            FROM EDW_PH_SELLOUT_ANALYSIS
            WHERE jj_year >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))
            AND   jj_grs_trd_sls IS NOT NULL
            AND   jj_grs_trd_sls > 0
            AND   (rka_cd IS NULL OR (rka_cd IS NOT NULL AND (rka_cd = ' ' OR rka_cd NOT IN ('RKA008','RKA032'))))) base
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON base.dstrbtr_grp_cd = b.dstrbtr_grp_cd
      GROUP BY jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               base.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               rka_cd,
               rka_nm
	  UNION ALL
	  SELECT 'NPI TDP' AS Identifier,
             jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,
              NULL AS sub_chnl_cd,
             base.dstrbtr_grp_cd,
             b.dstrbtr_grp_nm,
             dstrbtr_cust_cd AS trnsfrm_cust_id,
			     NULL AS invoice_dt,
             chnl_desc AS chnl_desc,
             global_prod_franchise AS franchise,
             global_prod_brand AS brand,
             global_prod_segment AS segment,
             rka_cd,
             rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 AS sku_cnt,
             0 AS tdp,
             COUNT(*) npi_tdp,
			 0 As msl_tdp,
			 
       0 As cnt_msl
      FROM (SELECT DISTINCT jj_year,
                   jj_qrtr,
                   jj_mnth_id,
                   jj_mnth_no,
                   cntry_nm,
                   account_grp,
                   sls_grp_desc,
                   dstrbtr_grp_cd,
                   dstrbtr_cust_cd,
                   chnl_desc,
                   global_prod_franchise,
                   global_prod_brand,
                   global_prod_segment,
                   rka_cd,
                   rka_nm,
                   sku
            FROM EDW_PH_SELLOUT_ANALYSIS sellout, ITG_MDS_PH_LAV_PRODUCT product
            WHERE jj_year >= (SELECT year(dateadd(day,-(SELECT CAST(parameter_value*365 AS INTEGER) FROM itg_query_parameters 
			WHERE parameter_name = 'PH_GT_SCORECARD_DATA_RETENTION_YEARS' AND country_code = 'PH'),current_timestamp())))
            AND   jj_grs_trd_sls IS NOT NULL
            AND   jj_grs_trd_sls > 0
            AND   (rka_cd IS NULL OR (rka_cd IS NOT NULL AND (rka_cd = ' ' OR rka_cd NOT IN ('RKA008','RKA032'))))
            AND   sellout.sku = product.item_cd
            AND   sellout.jj_mnth_id between product.npi_strt_period and to_char(dateadd(month,11,to_date(left(product.npi_strt_period,6), 'YYYYMM')),'YYYYMM')
            AND   product.npi_strt_period is not null
            AND   product.active='Y') base
        LEFT JOIN (SELECT dstrbtr_grp_cd,
                          dstrbtr_grp_nm
                   FROM itg_mds_ph_ref_distributors
                   WHERE UPPER(active) = 'Y') b ON base.dstrbtr_grp_cd = b.dstrbtr_grp_cd
      GROUP BY jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               base.dstrbtr_grp_cd,
               b.dstrbtr_grp_nm,
               trnsfrm_cust_id,
               chnl_desc,
               franchise,
               brand,
               segment,
               rka_cd,
               rka_nm	
               UNION ALL 
           SELECT 'MSL TDP' AS Identifier,
             jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,  
             sub_chnl_cd,
             dstrbtr_grp_cd,
             Null as dstrbtr_grp_nm,
             dstrbtr_cust_cd AS trnsfrm_cust_id,
             NULL AS invoice_dt,
             chnl_desc AS chnl_desc,
             global_prod_franchise AS franchise,
             global_prod_brand AS brand,
             global_prod_segment AS segment,
             rka_cd,
             rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 As sku_cnt,
             0 AS tdp,
             0 npi_tdp,
             COUNT(sku) AS msl_tdp,
			 
             0 AS cnt_msl
                                           
 FROM (
  SELECT a.*, b.tagging
  FROM MSLSellout a LEFT JOIN MSLProducts b
  ON
    a.jj_mnth_id = b.mnth_id
    AND a.sku = b.sku_code
    AND a.sub_chnl_cd = b.csg_code
  WHERE
    b.tagging = 'y'
  )
GROUP BY
  Identifier,
             jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,  
             sub_chnl_cd,
             dstrbtr_grp_cd,             
             dstrbtr_cust_cd ,             
             chnl_desc,
             global_prod_franchise,
             global_prod_brand,
             global_prod_segment,
             rka_cd,
             rka_nm

               UNION ALL 
             SELECT 'MSL COMPLIANCE' AS Identifier,
             jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             cntry_nm,
             account_grp,
             sls_grp_desc,  
             sub_chnl_cd,
             dstrbtr_grp_cd,
             Null as dstrbtr_grp_nm,
             dstrbtr_cust_cd AS trnsfrm_cust_id,
             NULL AS invoice_dt,
             chnl_desc AS chnl_desc,
             global_prod_franchise AS franchise,
             global_prod_brand AS brand,
             global_prod_segment AS segment,
             rka_cd,
             rka_nm,
             0 AS so_gts_val,
             0 AS si_gts_val,
             0 AS ret_val,
             0 AS unserved,
             0 AS promo_national,
             0 AS promo_local,
             0 AS npi,
             0 AS planned_visit,
             0 AS visited,
             0 AS Census,
             0 AS ontime,
             0 AS infull,
             0 AS otif,
             0 AS order_lines,
             0 AS inventory,
             0 AS average_sales,
             0 As sku_cnt,
             0 AS tdp,
             0  as npi_tdp,
			 0 as msl_tdp,
             cnt_msl AS cnt_msl
FROM 
(SELECT *
      FROM CntSelloutMSL a LEFT JOIN CntMSLProducts b
        ON a.jj_mnth_id = b.mnth_id
         AND a.sub_chnl_cd = b.csg_code
      ORDER BY
        sub_chnl_cd, dstrbtr_grp_cd)
GROUP BY
  jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               cntry_nm,
               account_grp,
               sls_grp_desc,
               sub_chnl_cd,
               dstrbtr_grp_cd,
               dstrbtr_grp_nm,
               dstrbtr_cust_cd,
               chnl_desc,
               franchise,
              brand,
               segment,
               rka_cd,
               rka_nm,
               cnt_sku,
               cnt_msl)
               
WHERE (rka_cd not in ('RKA008','RKA032') OR rka_cd IS NULL)
),
final as (
select
identifier::varchar(100) as identifier,
jj_year::varchar(4) as jj_year,
jj_qrtr::varchar(10) as jj_qrtr,
jj_mnth_id::varchar(10) as jj_mnth_id,
jj_mnth_no::varchar(10) as jj_mnth_no,
cntry_nm::varchar(20) as cntry_nm,
account_grp::varchar(50) as account_grp,
sls_grp_desc::varchar(100) as sls_grp_desc,
sub_chnl_cd::varchar(100) as sub_chnl_cd,
dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
dstrbtr_grp_nm::varchar(255) as dstrbtr_grp_nm,
trnsfrm_cust_id::varchar(255) as trnsfrm_cust_id,
invoice_dt::date as invoice_dt,
chnl_desc::varchar(255) as chnl_desc,
franchise::varchar(255) as franchise,
brand::varchar(550) as brand,
segment::varchar(550) as segment,
rka_cd::varchar(255) as rka_cd,
rka_nm::varchar(255) as rka_nm,
so_gts_val::number(18,4) as so_gts_val,
si_gts_val::number(18,4) as si_gts_val,
ret_val::number(18,4) as ret_val,
unserved::number(18,4) as unserved,
promo_national::number(18,4) as promo_national,
promo_local::number(18,4) as promo_local,
npi::number(18,4) as npi,
planned_visit::number(18,4) as planned_visit,
visited::number(18,4) as visited,
census::number(18,4) as census,
ontime::number(18,4) as ontime,
infull::number(18,4) as infull,
otif::number(18,4) as otif,
order_lines::number(18,4) as order_lines,
inventory::number(18,4) as inventory,
average_sales::number(18,4) as average_sales,
sku_cnt::number(18,4) as sku_cnt,
tdp::number(18,4) as tdp,
npi_tdp::number(18,4) as npi_tdp,
msl_tdp::number(18,4) as msl_tdp,
cnt_msl::number(18,4) as cnt_msl
from transformed
)
select * from final