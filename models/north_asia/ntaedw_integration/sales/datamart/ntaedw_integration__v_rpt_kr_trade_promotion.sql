with edw_kr_trade_promotion as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_KR_TRADE_PROMOTION
),
edw_intrm_calendar as (
select * from DEV_DNA_CORE.NTAEDW_INTEGRATION.EDW_INTRM_CALENDAR
),
itg_sap_billing_condition as (
select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_SAP_BILLING_CONDITION
),
v_intrm_reg_crncy_exch_fiscper as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.V_INTRM_REG_CRNCY_EXCH_FISCPER
),
edw_customer_attr_flat_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_ATTR_FLAT_DIM
),
edw_material_sales_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_MATERIAL_SALES_DIM
),
edw_product_attr_dim as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_PRODUCT_ATTR_DIM
),
final as (
SELECT 
  TRANS.promo_begin_date, 
  TRANS.promo_end_date, 
  TRANS.ctry_cd, 
  TRANS.crncy_cd, 
  TRANS.fisc_wk, 
  TRANS.fisc_yr, 
  TRANS.fisc_mon, 
  TRANS.yr AS year, 
  TRANS.wk AS week, 
  TRANS.mon AS month, 
  TRANS.customer_code, 
  cud.cust_nm AS customer_name, 
  cud.store_typ, 
  cud.channel AS customer_channel, 
  cud.sls_grp, 
  cud.sls_ofc, 
  cud.sls_ofc_desc, 
  TRANS.sap_sgrp AS sls_grp_cd, 
  TRANS.activity_name AS activity_name, 
  TRANS.product_code AS material_code, 
  ph.ean_num AS ean, 
  ph.prod_hier_l1, 
  ph.prod_hier_l2, 
  ph.prod_hier_l3, 
  ph.prod_hier_l4, 
  ph.prod_hier_l5, 
  ph.prod_hier_l6, 
  ph.prod_hier_l7, 
  ph.prod_hier_l8, 
  ph.prod_hier_l9, 
  COALESCE(
    SUM(TRANS.unit_tpr_price), 
    0
  ) AS unit_tpr_price, 
  SUM(TRANS.estimated_quantity) AS estimated_quantity, 
  SUM(TRANS.estimated_unit_price) AS estimated_unit_price, 
  SUM(TRANS.estimated_amount) AS estimated_amount_lcy, 
  SUM(
    TRANS.estimated_amount * exch_rate.ex_rt
  ) AS estimated_amount_usd, 
  COALESCE(
    SUM(TRANS.actual_quantity), 
    0
  ) AS actual_quantity, 
  COALESCE(
    SUM(TRANS.actual_quantity_pc), 
    0
  ) AS actual_quantity_pc, 
  COALESCE(
    SUM(TRANS.actual_unit_price), 
    0
  ) AS actual_unit_price, 
  COALESCE(
    SUM(TRANS.actual_amount), 
    0
  ) AS actual_amount_lcy, 
  COALESCE(
    SUM(
      TRANS.actual_amount * exch_rate.ex_rt
    ), 
    0
  ) AS actual_amount_usd, 
  TRANS.application_code, 
  TRANS.remark 
FROM 
  (
    SELECT 
      TP.line_begin_date AS promo_begin_date, 
      TP.line_end_date AS promo_end_date, 
      TP.ctry_cd, 
      TP.crncy_cd, 
      cal.fisc_wk_num AS fisc_wk, 
      cal.fisc_yr AS fisc_yr, 
      cal.pstng_per AS fisc_mon, 
      cal.fisc_per, 
      cal.cal_yr AS yr, 
      cal.wkday AS wk, 
      cal.cal_mo_2 AS mon, 
      TP.customer_code AS customer_code, 
      TP.sap_sgrp, 
      TP.activity_name AS activity_name, 
      TP.product_code, 
      SUM(unit_tpr_price) unit_tpr_price, 
      MAX(or_tp_qty) AS estimated_quantity, 
      MAX(or_tp_rebate_a) AS estimated_unit_price, 
      MAX(ttl_cost) AS estimated_amount, 
      SUM(actual_quantity) AS actual_quantity, 
      SUM(actual_quantity_pc) AS actual_quantity_pc, 
      SUM(unit_price) AS actual_unit_price, 
      SUM(actual_amount) AS actual_amount, 
      TP.application_code, 
      TP.remark AS remark 
    FROM 
      edw_kr_trade_promotion TP 
      LEFT JOIN edw_intrm_calendar cal ON TP.line_begin_date = cal.cal_day 
      LEFT JOIN (
        SELECT 
          calday, 
          loc_currcy, 
          sales_grp, 
          sold_to, 
          material, 
          SUM(actual_quantity) AS actual_quantity, 
          SUM(actual_quantity_pc) AS actual_quantity_pc, 
          SUM(unit_price) AS unit_price, 
          SUM(unit_price_GP - unit_price_TD) AS unit_tpr_price, 
          SUM(actual_amount) AS actual_amount 
        FROM 
          (
            SELECT 
              sbc.calday, 
              sbc.loc_currcy, 
              sbc.sales_grp, 
              sbc.knart, 
              sbc.sold_to, 
              sbc.material, 
              CASE WHEN knart = 'ZKSD' THEN SUM(sbc.inv_qty) ELSE 0 END AS actual_quantity, 
              CASE WHEN knart = 'ZKSD' THEN SUM(sbc.actual_quantity_pc) ELSE 0 END AS actual_quantity_pc, 
              CASE WHEN knart = 'ZKSD' THEN SUM(-1 * sbc.kprice) ELSE 0 END AS unit_price, 
              CASE WHEN knart = 'ZPR0' THEN SUM(sbc.kprice) ELSE 0 END AS unit_price_GP, 
              CASE WHEN knart = 'ZKTD' THEN SUM(sbc.kprice) ELSE 0 END AS unit_price_TD, 
              CASE WHEN knart = 'ZKSD' THEN SUM(-1 * sbc.knval) ELSE 0 END AS actual_amount 
            FROM 
              itg_sap_billing_condition sbc 
            WHERE 
              sbc.knart IN ('ZPR0', 'ZKTD', 'ZKSD') 
            GROUP BY 
              sbc.calday, 
              sbc.loc_currcy, 
              sbc.sales_grp, 
              sbc.knart, 
              sbc.sold_to, 
              sbc.material
          ) 
        GROUP BY 
          calday, 
          loc_currcy, 
          sales_grp, 
          sold_to, 
          material
      ) bc ON TP.sap_sgrp = bc.sales_grp 
      AND TP.customer_code = LTRIM (bc.sold_to, 0) 
      AND TP.crncy_cd = bc.loc_currcy 
      AND TP.product_code = LTRIM (bc.material, 0) 
      AND bc.calday BETWEEN TO_CHAR (
        TO_DATE (
          TP.line_begin_date::varchar, 'YYYY-MM-DD'
        ), 
        'YYYYMMDD'
      ) 
      AND TO_CHAR (
        TO_DATE (TP.line_end_date::varchar, 'YYYY-MM-DD'), 
        'YYYYMMDD'
      ) 
    WHERE 
      TP.customer_code IS NOT NULL 
    GROUP BY 
      TP.line_begin_date, 
      TP.line_end_date, 
      TP.ctry_cd, 
      TP.crncy_cd, 
      cal.fisc_wk_num, 
      cal.fisc_yr, 
      cal.fisc_per, 
      cal.pstng_per, 
      cal.cal_yr, 
      cal.wkday, 
      cal.cal_mo_2, 
      TP.customer_code, 
      TP.sap_sgrp, 
      TP.activity_name, 
      TP.product_code, 
      TP.application_code, 
      TP.remark
  ) TRANS 
  LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON TRANS.crncy_cd = exch_rate.from_crncy 
  AND TRANS.fisc_per = exch_rate.fisc_per 
  AND exch_rate.to_crncy = 'USD' 
  LEFT JOIN (
    SELECT 
      DISTINCT cntry, 
      sold_to_party, 
      cust_nm, 
      store_typ, 
      channel, 
      sls_grp, 
      sls_ofc, 
      sls_ofc_desc, 
      sls_grp_cd 
    FROM 
      edw_customer_attr_flat_dim 
    WHERE 
      cntry = 'Korea' 
      AND sold_to_party <> ''
  ) cud ON TRANS.customer_code = cud.sold_to_party 
  AND TRANS.sap_sgrp = cud.sls_grp_cd 
  LEFT JOIN (
    SELECT 
      matl_num, 
      ean_num, 
      prod_hier_l1, 
      prod_hier_l2, 
      prod_hier_l3, 
      prod_hier_l4, 
      prod_hier_l5, 
      prod_hier_l6, 
      prod_hier_l7, 
      prod_hier_l8, 
      prod_hier_l9 
    FROM 
      (
        SELECT 
          DISTINCT msd.matl_num, 
          msd.ean_num, 
          pd.prod_hier_l1, 
          pd.prod_hier_l2, 
          pd.prod_hier_l3, 
          pd.prod_hier_l4, 
          pd.prod_hier_l5, 
          pd.prod_hier_l6, 
          pd.prod_hier_l7, 
          pd.prod_hier_l8, 
          pd.prod_hier_l9, 
          ROW_NUMBER() OVER (
            PARTITION BY msd.matl_num 
            ORDER BY 
              msd.ean_num DESC
          ) rn 
        FROM 
          edw_material_sales_dim msd 
          LEFT JOIN (
            SELECT 
              DISTINCT aw_remote_key, 
              awrefs_prod_remotekey, 
              awrefs_buss_unit, 
              sap_matl_num, 
              cntry, 
              ean, 
              prod_hier_l1, 
              prod_hier_l2, 
              prod_hier_l3, 
              prod_hier_l4, 
              prod_hier_l5, 
              prod_hier_l6, 
              prod_hier_l7, 
              prod_hier_l8, 
              prod_hier_l9 
            FROM 
              edw_product_attr_dim 
            WHERE 
              cntry = 'KR'
          ) pd ON msd.ean_num = pd.ean 
        WHERE 
          sls_org IN ('320A', '3200', '320S') 
          AND ean_num <> '' 
          AND prod_hier_l1 <> ''
      ) 
    WHERE 
      rn = 1
  ) ph ON TRANS.product_code = LTRIM (ph.matl_num, 0) 
GROUP BY 
  TRANS.promo_begin_date, 
  TRANS.promo_end_date, 
  TRANS.ctry_cd, 
  TRANS.crncy_cd, 
  TRANS.fisc_wk, 
  TRANS.fisc_yr, 
  TRANS.fisc_mon, 
  TRANS.yr, 
  TRANS.wk, 
  TRANS.mon, 
  TRANS.customer_code, 
  cud.cust_nm, 
  cud.store_typ, 
  cud.channel, 
  cud.sls_grp, 
  cud.sls_ofc, 
  cud.sls_ofc_desc, 
  TRANS.sap_sgrp, 
  TRANS.activity_name, 
  TRANS.product_code, 
  ph.ean_num, 
  ph.prod_hier_l1, 
  ph.prod_hier_l2, 
  ph.prod_hier_l3, 
  ph.prod_hier_l4, 
  ph.prod_hier_l5, 
  ph.prod_hier_l6, 
  ph.prod_hier_l7, 
  ph.prod_hier_l8, 
  ph.prod_hier_l9, 
  TRANS.application_code, 
  TRANS.remark 
UNION ALL 
SELECT 
  TRANS.promo_begin_date, 
  TRANS.promo_end_date, 
  TRANS.ctry_cd, 
  TRANS.crncy_cd, 
  TRANS.fisc_wk, 
  TRANS.fisc_yr, 
  TRANS.fisc_mon, 
  TRANS.yr AS year, 
  TRANS.wk AS week, 
  TRANS.mon AS month, 
  NULL AS customer_code, 
  NULL AS customer_name, 
  cud.store_typ, 
  cud.channel AS customer_channel, 
  cud.sls_grp, 
  cud.sls_ofc, 
  cud.sls_ofc_desc, 
  TRANS.sap_sgrp AS sls_grp_cd, 
  TRANS.activity_name AS activity_name, 
  TRANS.product_code AS material_code, 
  ph.ean_num AS ean, 
  ph.prod_hier_l1, 
  ph.prod_hier_l2, 
  ph.prod_hier_l3, 
  ph.prod_hier_l4, 
  ph.prod_hier_l5, 
  ph.prod_hier_l6, 
  ph.prod_hier_l7, 
  ph.prod_hier_l8, 
  ph.prod_hier_l9, 
  COALESCE(
    SUM(TRANS.unit_tpr_price), 
    0
  ) AS unit_tpr_price, 
  SUM(TRANS.estimated_quantity) AS estimated_quantity, 
  SUM(TRANS.estimated_unit_price) AS estimated_unit_price, 
  SUM(TRANS.estimated_amount) AS estimated_amount_lcy, 
  SUM(
    TRANS.estimated_amount * exch_rate.ex_rt
  ) AS estimated_amount_usd, 
  COALESCE(
    SUM(TRANS.actual_quantity), 
    0
  ) AS actual_quantity, 
  COALESCE(
    SUM(TRANS.actual_quantity_pc), 
    0
  ) AS actual_quantity_pc, 
  COALESCE(
    SUM(TRANS.actual_unit_price), 
    0
  ) AS actual_unit_price, 
  COALESCE(
    SUM(TRANS.actual_amount), 
    0
  ) AS actual_amount_lcy, 
  COALESCE(
    SUM(
      TRANS.actual_amount * exch_rate.ex_rt
    ), 
    0
  ) AS actual_amount_usd, 
  TRANS.application_code, 
  TRANS.remark 
FROM 
  (
    SELECT 
      TP.line_begin_date AS promo_begin_date, 
      TP.line_end_date AS promo_end_date, 
      TP.ctry_cd, 
      TP.crncy_cd, 
      cal.fisc_wk_num AS fisc_wk, 
      cal.fisc_yr AS fisc_yr, 
      cal.pstng_per AS fisc_mon, 
      cal.fisc_per, 
      cal.cal_yr AS yr, 
      cal.wkday AS wk, 
      cal.cal_mo_2 AS mon, 
      'NA' AS customer_code, 
      TP.sap_sgrp, 
      TP.activity_name AS activity_name, 
      TP.product_code, 
      SUM(unit_tpr_price) unit_tpr_price, 
      MAX(or_tp_qty) AS estimated_quantity, 
      MAX(or_tp_rebate_a) AS estimated_unit_price, 
      MAX(ttl_cost) AS estimated_amount, 
      SUM(actual_quantity) AS actual_quantity, 
      SUM(actual_quantity_pc) AS actual_quantity_pc, 
      SUM(unit_price) AS actual_unit_price, 
      SUM(actual_amount) AS actual_amount, 
      TP.application_code, 
      TP.remark AS remark 
    FROM 
      edw_kr_trade_promotion TP 
      LEFT JOIN edw_intrm_calendar cal ON TP.line_begin_date = cal.cal_day 
      LEFT JOIN (
        SELECT 
          calday, 
          loc_currcy, 
          sales_grp, 
          --sold_to,
          material, 
          SUM(actual_quantity) AS actual_quantity, 
          SUM(actual_quantity_pc) AS actual_quantity_pc, 
          SUM(unit_price) AS unit_price, 
          SUM(unit_price_GP - unit_price_TD) AS unit_tpr_price, 
          SUM(actual_amount) AS actual_amount 
        FROM 
          (
            SELECT 
              sbc.calday, 
              sbc.loc_currcy, 
              sbc.sales_grp, 
              sbc.knart, 
              --sbc.sold_to,
              sbc.material, 
              CASE WHEN knart = 'ZKSD' THEN SUM(sbc.inv_qty) ELSE 0 END AS actual_quantity, 
              CASE WHEN knart = 'ZKSD' THEN SUM(sbc.actual_quantity_pc) ELSE 0 END AS actual_quantity_pc, 
              CASE WHEN knart = 'ZKSD' THEN SUM(-1 * sbc.kprice) ELSE 0 END AS unit_price, 
              CASE WHEN knart = 'ZPR0' THEN SUM(sbc.kprice) ELSE 0 END AS unit_price_GP, 
              CASE WHEN knart = 'ZKTD' THEN SUM(sbc.kprice) ELSE 0 END AS unit_price_TD, 
              CASE WHEN knart = 'ZKSD' THEN SUM(-1 * sbc.knval) ELSE 0 END AS actual_amount 
            FROM 
              itg_sap_billing_condition sbc 
            WHERE 
              sbc.knart IN ('ZPR0', 'ZKTD', 'ZKSD') 
            GROUP BY 
              sbc.calday, 
              sbc.loc_currcy, 
              sbc.sales_grp, 
              sbc.knart, 
              --sbc.sold_to,
              sbc.material
          ) 
        GROUP BY 
          calday, 
          loc_currcy, 
          sales_grp, 
          --sold_to,
          material
      ) bc ON TP.sap_sgrp = bc.sales_grp 
      AND --AND TP.customer_code = LTRIM (bc.sold_to,0) /*Grouping at sales group level*/
      TP.crncy_cd = bc.loc_currcy 
      AND TP.product_code = LTRIM (bc.material, 0) 
      AND bc.calday BETWEEN TO_CHAR (
        TO_DATE (
          TP.line_begin_date::varchar, 'YYYY-MM-DD'
        ), 
        'YYYYMMDD'
      ) 
      AND TO_CHAR (
        TO_DATE (TP.line_end_date::varchar, 'YYYY-MM-DD'), 
        'YYYYMMDD'
      ) 
    WHERE 
      TP.customer_code IS NULL 
    GROUP BY 
      TP.line_begin_date, 
      TP.line_end_date, 
      TP.ctry_cd, 
      TP.crncy_cd, 
      cal.fisc_wk_num, 
      cal.fisc_yr, 
      cal.fisc_per, 
      cal.pstng_per, 
      cal.cal_yr, 
      cal.wkday, 
      cal.cal_mo_2, 
      --'NA' AS TP.customer_code,
      TP.sap_sgrp, 
      TP.activity_name, 
      TP.product_code, 
      TP.application_code, 
      TP.remark
  ) TRANS 
  LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON TRANS.crncy_cd = exch_rate.from_crncy 
  AND TRANS.fisc_per = exch_rate.fisc_per 
  AND exch_rate.to_crncy = 'USD' 
  LEFT JOIN (
    SELECT 
      DISTINCT cntry, 
      'NA' AS sold_to_party, 
      'NA' AS cust_nm, 
      store_typ, 
      channel, 
      sls_grp, 
      sls_ofc, 
      sls_ofc_desc, 
      sls_grp_cd 
    FROM 
      edw_customer_attr_flat_dim 
    WHERE 
      cntry = 'Korea' 
      AND sold_to_party <> '' 
      AND sls_grp_cd <> ''
  ) cud ON --TRANS.customer_code = cud.sold_to_party AND
  TRANS.sap_sgrp = cud.sls_grp_cd 
  LEFT JOIN (
    SELECT 
      matl_num, 
      ean_num, 
      prod_hier_l1, 
      prod_hier_l2, 
      prod_hier_l3, 
      prod_hier_l4, 
      prod_hier_l5, 
      prod_hier_l6, 
      prod_hier_l7, 
      prod_hier_l8, 
      prod_hier_l9 
    FROM 
      (
        SELECT 
          DISTINCT msd.matl_num, 
          msd.ean_num, 
          pd.prod_hier_l1, 
          pd.prod_hier_l2, 
          pd.prod_hier_l3, 
          pd.prod_hier_l4, 
          pd.prod_hier_l5, 
          pd.prod_hier_l6, 
          pd.prod_hier_l7, 
          pd.prod_hier_l8, 
          pd.prod_hier_l9, 
          ROW_NUMBER() OVER (
            PARTITION BY msd.matl_num 
            ORDER BY 
              msd.ean_num DESC
          ) rn 
        FROM 
          edw_material_sales_dim msd 
          LEFT JOIN (
            SELECT 
              DISTINCT aw_remote_key, 
              awrefs_prod_remotekey, 
              awrefs_buss_unit, 
              sap_matl_num, 
              cntry, 
              ean, 
              prod_hier_l1, 
              prod_hier_l2, 
              prod_hier_l3, 
              prod_hier_l4, 
              prod_hier_l5, 
              prod_hier_l6, 
              prod_hier_l7, 
              prod_hier_l8, 
              prod_hier_l9 
            FROM 
              edw_product_attr_dim 
            WHERE 
              cntry = 'KR'
          ) pd ON msd.ean_num = pd.ean 
        WHERE 
          sls_org IN ('320A', '3200', '320S') 
          AND ean_num <> '' 
          AND prod_hier_l1 <> ''
      ) 
    WHERE 
      rn = 1
  ) ph ON TRANS.product_code = LTRIM (ph.matl_num, 0) 
GROUP BY 
  TRANS.promo_begin_date, 
  TRANS.promo_end_date, 
  TRANS.ctry_cd, 
  TRANS.crncy_cd, 
  TRANS.fisc_wk, 
  TRANS.fisc_yr, 
  TRANS.fisc_mon, 
  TRANS.yr, 
  TRANS.wk, 
  TRANS.mon, 
  --TRANS.customer_code,
  cud.cust_nm, 
  cud.store_typ, 
  cud.channel, 
  cud.sls_grp, 
  cud.sls_ofc, 
  cud.sls_ofc_desc, 
  TRANS.sap_sgrp, 
  TRANS.activity_name, 
  TRANS.product_code, 
  ph.ean_num, 
  ph.prod_hier_l1, 
  ph.prod_hier_l2, 
  ph.prod_hier_l3, 
  ph.prod_hier_l4, 
  ph.prod_hier_l5, 
  ph.prod_hier_l6, 
  ph.prod_hier_l7, 
  ph.prod_hier_l8, 
  ph.prod_hier_l9, 
  TRANS.application_code, 
  TRANS.remark 
)
select count(*) from final
union all
select count(*) from DEV_DNA_CORE.NTAEDW_INTEGRATION.V_RPT_KR_TRADE_PROMOTION;
