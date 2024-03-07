with EDW_VW_OS_CUSTOMER_DIM as(
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_VW_OS_CUSTOMER_DIM
),
sdl_mds_th_customer_product_code as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_customer_product_code') }}
),
sdl_mds_th_product_master as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_product_master') }}
),
edw_list_price as(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
wks_th_watsons as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons') }}
),
EDW_BILLING_FACT as(
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_BILLING_FACT
),
edw_vw_os_time_dim as(
    select * from DEV_DNA_CORE.OSEEDW_ACCESS.EDW_VW_OS_TIME_DIM 
),
a as(
    SELECT
      CAST('108835' AS VARCHAR) AS sold_to_code,
      CASE
        WHEN date IS NULL
        THEN TO_CHAR(
          CAST(TO_DATE(SUBSTRING(file_name, 1, 4) || '-' || SUBSTRING(file_name, 9, 3), 'YYYY-MON') AS TIMESTAMPNTZ),
          'yyyymm'
        )
        ELSE TO_CHAR(CAST(TO_DATE(date, 'YYYY-MM-DD') AS TIMESTAMPNTZ), 'yyyymm')
      END /* date, */ /* to_char(to_date(date,'YYYY-MM-DD'),'YYYYMM') as month, */ AS MONTH,
      COALESCE(NULLIF(c.matl_num, ''), 'NA') AS matl_num,
      a.item,
      item_desc,
      b.barcode,
      SUM(wh_soh) AS wh_soh,
      SUM(store_total_stock) AS store_total_stock,
      SUM(total_stock_qty) AS total_stock_qty_raw,
      SUM(total_stock_qty * COALESCE(retailer_unit_conversion, 1)) AS total_stock_qty,
      SUM(total_stock_qty * COALESCE(retailer_unit_conversion, 1) * list_price) AS total_stock_val,
      SUM(sale_avg_qty_13weeks) AS sale_avg_qty_13weeks_raw,
      SUM(sale_avg_qty_13weeks * COALESCE(retailer_unit_conversion, 1)) AS sale_avg_qty_13weeks,
      SUM(sale_avg_qty_13weeks * COALESCE(retailer_unit_conversion, 1) * list_price) AS sale_avg_val_13weeks
    FROM wks_th_watsons AS a
    LEFT JOIN sdl_mds_th_customer_product_code AS b
      ON a.item = b.item
    LEFT JOIN (
      SELECT DISTINCT
        code AS matl_num,
        barcode AS barcd,
        retailer_unit_conversion,
        createdate
      FROM (
        SELECT
          barcode,
          code,
          retailer_unit_conversion,
          createdate,
          ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY createdate DESC NULLS LAST, code) AS rnk
        FROM sdl_mds_th_product_master
        WHERE
          barcode <> ''
      )
      WHERE
        rnk = 1
    ) AS c
      ON b.barcode = c.barcd
    LEFT JOIN (
      SELECT
        *
      FROM (
        SELECT
          LTRIM(CAST(edw_list_price.material AS TEXT), CAST(CAST(0 AS VARCHAR) AS TEXT)) AS material,
          edw_list_price.amount AS list_price,
          ROW_NUMBER() OVER (PARTITION BY LTRIM(CAST(edw_list_price.material AS TEXT), CAST(CAST(0 AS VARCHAR) AS TEXT)) ORDER BY TO_DATE(CAST(edw_list_price.valid_to AS TEXT), CAST(CAST('YYYYMMDD' AS VARCHAR) AS TEXT)) DESC, TO_DATE(CAST(edw_list_price.dt_from AS TEXT), CAST(CAST('YYYYMMDD' AS VARCHAR) AS TEXT)) DESC) AS rn
        FROM edw_list_price
        WHERE
          CAST(edw_list_price.sls_org AS TEXT) IN ('2400')
      )
      WHERE
        rn = 1
    ) AS d
      ON c.matl_num = d.material
    GROUP BY
      CASE
        WHEN date IS NULL
        THEN TO_CHAR(
          CAST(TO_DATE(SUBSTRING(file_name, 1, 4) || '-' || SUBSTRING(file_name, 9, 3), 'YYYY-MON') AS TIMESTAMPNTZ),
          'yyyymm'
        )
        ELSE TO_CHAR(CAST(TO_DATE(date, 'YYYY-MM-DD') AS TIMESTAMPNTZ), 'yyyymm')
      END, /* date, */
      a.item,
      item_desc,
      b.barcode,
      c.matl_num
),
c as(
    SELECT
      sap_cust_id,
      sap_prnt_cust_key,
      sap_prnt_cust_desc
    FROM EDW_VW_OS_CUSTOMER_DIM
    WHERE
      SAP_CNTRY_CD = 'TH'
),
union1 as(
  SELECT
    sap_prnt_cust_key,
    sap_prnt_cust_desc,
    CAST('108835' AS VARCHAR) AS sold_to_code,
    mnth_id, /* null, */
    matl_num,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    SUM(bill_qty_pc) AS bill_qty_pc,
    SUM(grs_trd_sls) AS grs_trd_sls
  FROM (
    SELECT
      BILL_DT,
      sold_to,
      matl_num,
      SUM(bill_qty_pc) AS bill_qty_pc,
      SUM(grs_trd_sls) AS grs_trd_sls
    FROM (
      SELECT
        BILL_DT,
        LTRIM(SOLD_TO, '0') AS SOLD_TO,
        LTRIM(MATERIAL, '0') AS MATL_NUM,
        BILL_TYPE,
        SUM(BILL_QTY) AS BILL_QTY_PC,
        SUM(SUBTOTAL_1) AS GRS_TRD_SLS
      FROM EDW_BILLING_FACT
      WHERE
        LTRIM(SOLD_TO, '0') IN ('108835')
        AND SLS_ORG IN ('2400', '2500')
        AND BILL_TYPE = 'ZF2L'
        AND CAST(TO_CHAR(CAST(BILL_DT AS TIMESTAMPNTZ), 'yyyy') AS INT) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
      GROUP BY
        BILL_DT,
        BILL_TYPE,
        SOLD_TO,
        MATL_NUM
    )
    GROUP BY
      BILL_DT,
      sold_to,
      matl_num
  ) AS T1
  JOIN (
    SELECT
      *
    FROM edw_vw_os_time_dim
  ) AS T2
    ON T1.BILL_DT = T2.cal_daTE
  LEFT JOIN (
    SELECT
      *
    FROM EDW_VW_OS_CUSTOMER_DIM
    WHERE
      SAP_CNTRY_CD = 'TH'
  ) AS c
    ON LTRIM(T1.sold_to, 0) = LTRIM(c.sap_cust_id, 0)
  GROUP BY
    sap_prnt_cust_key,
    sap_prnt_cust_desc,
    mnth_id,
    matl_num
),
transformed as(
SELECT
  sap_prnt_cust_key,
  sap_prnt_cust_desc,
  month, /* sold_to_code,date, */
  matl_num, /* item,item_desc,barcode, */
  SUM(wh_soh) AS wh_soh,
  SUM(store_total_stock) AS store_total_stock,
  SUM(total_stock_qty_raw) AS total_stock_qty_raw,
  SUM(total_stock_qty) AS total_stock_qty,
  SUM(total_stock_val) AS total_stock_val,
  SUM(sale_avg_qty_13weeks_raw) AS sale_avg_qty_13aweeks_raw,
  SUM(sale_avg_qty_13weeks) AS sale_avg_qty_13weeks,
  SUM(sale_avg_val_13weeks) AS sale_avg_val_13weeks,
  SUM(sellin_qty) AS sellin_qty,
  SUM(sellin_val) AS sellin_val
FROM (
  SELECT
    sap_prnt_cust_key,
    sap_prnt_cust_desc,
    a.*,
    NULL AS sellin_qty,
    NULL AS sellin_val
  FROM a AS a
  LEFT JOIN c AS c
    ON CAST(a.sold_to_code AS VARCHAR(10)) = CAST(c.sap_cust_id AS VARCHAR(10))
  UNION ALL
  select * from union1

)
GROUP BY
  sap_prnt_cust_key,
  sap_prnt_cust_desc, /* sold_to_code,date, */
  month,
  matl_num
 )
 select * from transformed