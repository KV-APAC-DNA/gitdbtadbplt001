with edw_billing_fact as (
      select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_BILLING_FACT
),
transformed as(
    select distinct
  CASE
    WHEN (
      CAST((
        edw_billing_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MY' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END AS cntry_key,
  CASE
    WHEN (
      CAST((
        edw_billing_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MALAYSIA' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END AS cntry_nm,
  edw_billing_fact.bill_dt,
  LTRIM(CAST((
    edw_billing_fact.bill_num
  ) AS TEXT), CAST('0' AS TEXT)) AS bill_num,
  LTRIM(CAST((
    edw_billing_fact.bill_item
  ) AS TEXT), CAST('0' AS TEXT)) AS bill_item,
  edw_billing_fact.bill_type,
  LTRIM(CAST((
    edw_billing_fact.doc_num
  ) AS TEXT), CAST('0' AS TEXT)) AS sls_doc_num,
  LTRIM(CAST((
    edw_billing_fact.s_ord_item
  ) AS TEXT), CAST('0' AS TEXT)) AS sls_doc_item,
  edw_billing_fact.doc_currcy AS doc_curr,
  edw_billing_fact.doc_categ AS sd_doc_catgy,
  LTRIM(CAST((
    edw_billing_fact.sold_to
  ) AS TEXT), CAST('0' AS TEXT)) AS sold_to,
  LTRIM(CAST((
    edw_billing_fact.material
  ) AS TEXT), CAST('0' AS TEXT)) AS matl_num,
  edw_billing_fact.sls_org,
  ABS(edw_billing_fact.exchg_rate) AS exchg_rate,
  SUM(edw_billing_fact.bill_qty) AS bill_qty_pc,
  SUM(edw_billing_fact.subtotal_1) AS grs_trd_sls,
  SUM(edw_billing_fact.subtotal_2) AS subtotal_2,
  SUM(edw_billing_fact.subtotal_3) AS subtotal_3,
  SUM(edw_billing_fact.subtotal_4) AS subtotal_4,
  SUM(edw_billing_fact.subtotal_5) AS net_amt,
  SUM(edw_billing_fact.subtotal_6) AS est_nts,
  SUM(edw_billing_fact.netval_inv) AS net_val,
  SUM(edw_billing_fact.gross_val) AS gross_val
FROM edw_billing_fact
WHERE
CAST((edw_billing_fact.sls_org) AS TEXT) = CAST('2100' AS TEXT)
GROUP BY
  CASE
    WHEN (
      CAST((
        edw_billing_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MY' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END,
  CASE
    WHEN (
      CAST((
        edw_billing_fact.sls_org
      ) AS TEXT) = CAST('2100' AS TEXT)
    )
    THEN CAST('MALAYSIA' AS TEXT)
    ELSE CAST(NULL AS TEXT)
  END,
  edw_billing_fact.bill_dt,
  edw_billing_fact.bill_num,
  edw_billing_fact.bill_item,
  edw_billing_fact.bill_type,
  LTRIM(CAST((
    edw_billing_fact.doc_num
  ) AS TEXT), CAST('0' AS TEXT)),
  LTRIM(CAST((
    edw_billing_fact.s_ord_item
  ) AS TEXT), CAST('0' AS TEXT)),
  edw_billing_fact.doc_currcy,
  edw_billing_fact.doc_categ,
  edw_billing_fact.sold_to,
  LTRIM(CAST((
    edw_billing_fact.material
  ) AS TEXT), CAST('0' AS TEXT)),
  edw_billing_fact.sls_org,
  ABS(edw_billing_fact.exchg_rate)
)
select * from transformed