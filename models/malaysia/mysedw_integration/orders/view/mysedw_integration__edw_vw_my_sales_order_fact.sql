with edw_sales_order_fact as(
    select * from {{ ref('aspedw_integration__edw_sales_order_fact') }}
),

transformed as(
    SELECT DISTINCT
  CASE
    WHEN (
      CAST((
        edw_sales_order_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MY' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END AS cntry_key,
  CASE
    WHEN (
      CAST((
        edw_sales_order_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MALAYSIA' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END AS cntry_nm,
  edw_sales_order_fact.doc_dt,
  edw_sales_order_fact.bill_dt,
  LTRIM(CAST((
    edw_sales_order_fact.doc_num
  ) AS TEXT), CAST('0' AS TEXT)) AS sls_doc_num,
  LTRIM(CAST((
    edw_sales_order_fact.s_ord_item
  ) AS TEXT), CAST('0' AS TEXT)) AS sls_doc_item,
  edw_sales_order_fact.created_on AS doc_creation_dt,
  edw_sales_order_fact.doc_type,
  edw_sales_order_fact.doc_categ AS sd_doc_catgy,
  CAST(NULL AS TEXT) AS po_num,
  LTRIM(CAST((
    edw_sales_order_fact.sold_to
  ) AS TEXT), CAST('0' AS TEXT)) AS sold_to,
  LTRIM(CAST((
    edw_sales_order_fact.material
  ) AS TEXT), CAST('0' AS TEXT)) AS matl_num,
  edw_sales_order_fact.sls_org,
  edw_sales_order_fact.base_uom,
  edw_sales_order_fact.order_curr AS purch_ord_curr,
  edw_sales_order_fact.zreq_dt AS req_delvry_dt,
  edw_sales_order_fact.rejectn_st,
  edw_sales_order_fact.reason_rej AS rejectn_cd,
  CAST(NULL AS TEXT) AS rejectn_desc,
  ABS(edw_sales_order_fact.exchg_rate) AS exchg_rate,
  SUM(edw_sales_order_fact.zordqtybu) AS ord_qty,
  SUM(edw_sales_order_fact.net_price) AS net_price,
  SUM(edw_sales_order_fact.subtotal_1) AS grs_trd_sls,
  SUM(edw_sales_order_fact.subtotal_2) AS subtotal_2,
  SUM(edw_sales_order_fact.subtotal_3) AS subtotal_3,
  SUM(edw_sales_order_fact.subtotal_4) AS subtotal_4,
  SUM(edw_sales_order_fact.subtotal_5) AS net_amt,
  SUM(edw_sales_order_fact.subtotal_6) AS est_nts
FROM edw_sales_order_fact
WHERE CAST((edw_sales_order_fact.sls_org) AS TEXT) = CAST('2100' AS TEXT)
GROUP BY
  CASE
    WHEN (
      CAST((
        edw_sales_order_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MY' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END,
  CASE
    WHEN (
      CAST((
        edw_sales_order_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MALAYSIA' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END,
  edw_sales_order_fact.doc_dt,
  edw_sales_order_fact.bill_dt,
  LTRIM(CAST((
    edw_sales_order_fact.doc_num
  ) AS TEXT), CAST('0' AS TEXT)),
  LTRIM(CAST((
    edw_sales_order_fact.s_ord_item
  ) AS TEXT), CAST('0' AS TEXT)),
  edw_sales_order_fact.created_on,
  edw_sales_order_fact.doc_type,
  edw_sales_order_fact.doc_categ,
  10,
  edw_sales_order_fact.sold_to,
  LTRIM(CAST((
    edw_sales_order_fact.material
  ) AS TEXT), CAST('0' AS TEXT)),
  edw_sales_order_fact.sls_org,
  edw_sales_order_fact.base_uom,
  edw_sales_order_fact.order_curr,
  edw_sales_order_fact.zreq_dt,
  edw_sales_order_fact.rejectn_st,
  edw_sales_order_fact.reason_rej,
  19,
  ABS(edw_sales_order_fact.exchg_rate)
)
select * from transformed