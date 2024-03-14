with edw_vw_my_sales_order_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_sales_order_fact') }}
),
edw_vw_my_billing_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_billing_fact') }}
),
from_edw_vw_my_sales_order_fact as(
     SELECT
      edw_vw_my_sales_order_fact.cntry_key,
      edw_vw_my_sales_order_fact.cntry_nm,
      edw_vw_my_sales_order_fact.doc_dt,
      edw_vw_my_sales_order_fact.bill_dt,
      edw_vw_my_sales_order_fact.sls_doc_num,
      edw_vw_my_sales_order_fact.sls_doc_item,
      edw_vw_my_sales_order_fact.doc_creation_dt,
      edw_vw_my_sales_order_fact.doc_type,
      edw_vw_my_sales_order_fact.sd_doc_catgy,
      edw_vw_my_sales_order_fact.po_num,
      edw_vw_my_sales_order_fact.sold_to,
      edw_vw_my_sales_order_fact.matl_num,
      edw_vw_my_sales_order_fact.sls_org,
      edw_vw_my_sales_order_fact.base_uom,
      edw_vw_my_sales_order_fact.purch_ord_curr,
      edw_vw_my_sales_order_fact.req_delvry_dt,
      edw_vw_my_sales_order_fact.rejectn_st,
      edw_vw_my_sales_order_fact.rejectn_cd,
      edw_vw_my_sales_order_fact.rejectn_desc,
      edw_vw_my_sales_order_fact.exchg_rate,
      edw_vw_my_sales_order_fact.ord_qty,
      edw_vw_my_sales_order_fact.net_price,
      edw_vw_my_sales_order_fact.grs_trd_sls,
      edw_vw_my_sales_order_fact.subtotal_2,
      edw_vw_my_sales_order_fact.subtotal_3,
      edw_vw_my_sales_order_fact.subtotal_4,
      edw_vw_my_sales_order_fact.net_amt,
      edw_vw_my_sales_order_fact.est_nts
    FROM edw_vw_my_sales_order_fact
    WHERE
      (
        CAST((
          edw_vw_my_sales_order_fact.sls_org
        ) AS TEXT) = CAST('2100' AS TEXT)
      )
),
from_edw_vw_my_billing_fact as(
    SELECT
      edw_vw_my_billing_fact.cntry_key,
      edw_vw_my_billing_fact.cntry_nm,
      edw_vw_my_billing_fact.bill_dt,
      edw_vw_my_billing_fact.bill_num,
      edw_vw_my_billing_fact.bill_item,
      edw_vw_my_billing_fact.bill_type,
      edw_vw_my_billing_fact.sls_doc_num,
      edw_vw_my_billing_fact.sls_doc_item,
      edw_vw_my_billing_fact.doc_curr,
      edw_vw_my_billing_fact.sd_doc_catgy,
      edw_vw_my_billing_fact.sold_to,
      edw_vw_my_billing_fact.matl_num,
      edw_vw_my_billing_fact.sls_org,
      edw_vw_my_billing_fact.exchg_rate,
      edw_vw_my_billing_fact.bill_qty_pc,
      edw_vw_my_billing_fact.grs_trd_sls,
      edw_vw_my_billing_fact.subtotal_2,
      edw_vw_my_billing_fact.subtotal_3,
      edw_vw_my_billing_fact.subtotal_4,
      edw_vw_my_billing_fact.net_amt,
      edw_vw_my_billing_fact.est_nts,
      edw_vw_my_billing_fact.net_val,
      edw_vw_my_billing_fact.gross_val
    FROM edw_vw_my_billing_fact
    WHERE
      (
        CAST((
          edw_vw_my_billing_fact.sls_org
        ) AS TEXT) = CAST('2100' AS TEXT)
      )
),
transformed as(
    SELECT
  osof.doc_dt,
  osof.po_num,
  osof.sls_doc_num,
  osof.sls_doc_item,
  osof.doc_type AS sls_doc_type,
  obf.bill_dt,
  obf.bill_num,
  obf.bill_item,
  osof.doc_creation_dt,
  osof.sold_to,
  osof.matl_num,
  osof.req_delvry_dt,
  osof.rejectn_st,
  osof.rejectn_cd,
  osof.rejectn_desc,
  osof.ord_qty,
  (
    osof.net_price * ABS(osof.exchg_rate)
  ) AS ord_net_price,
  (
    osof.grs_trd_sls * ABS(osof.exchg_rate)
  ) AS ord_grs_trd_sls,
  (
    osof.subtotal_2 * ABS(osof.exchg_rate)
  ) AS ord_subtotal_2,
  (
    osof.subtotal_3 * ABS(osof.exchg_rate)
  ) AS ord_subtotal_3,
  (
    osof.subtotal_4 * ABS(osof.exchg_rate)
  ) AS ord_subtotal_4,
  (
    osof.net_amt * ABS(osof.exchg_rate)
  ) AS ord_net_amt,
  (
    osof.est_nts * ABS(osof.exchg_rate)
  ) AS ord_est_nts,
  obf.bill_qty_pc,
  (
    obf.grs_trd_sls * ABS(obf.exchg_rate)
  ) AS bill_grs_trd_sls,
  (
    obf.subtotal_2 * ABS(obf.exchg_rate)
  ) AS bill_subtotal_2,
  (
    obf.subtotal_3 * ABS(obf.exchg_rate)
  ) AS bill_subtotal_3,
  (
    obf.subtotal_4 * ABS(obf.exchg_rate)
  ) AS bill_subtotal_4,
  (
    obf.net_amt * ABS(obf.exchg_rate)
  ) AS bill_net_amt,
  (
    obf.est_nts * ABS(obf.exchg_rate)
  ) AS bill_est_nts,
  (
    obf.net_val * ABS(obf.exchg_rate)
  ) AS bill_net_val,
  (
    obf.gross_val * ABS(obf.exchg_rate)
  ) AS bill_gross_val
FROM (
  (
   from_edw_vw_my_sales_order_fact
  ) AS osof
  LEFT JOIN (
    from_edw_vw_my_billing_fact
  ) AS obf
    ON (
      (
        (
          LTRIM(osof.sls_doc_num, CAST('0' AS TEXT)) = LTRIM(obf.sls_doc_num, CAST('0' AS TEXT))
        )
        AND (
          LTRIM(osof.sls_doc_item, CAST('0' AS TEXT)) = LTRIM(obf.sls_doc_item, CAST('0' AS TEXT))
        )
      )
    )
)

),
final as(
    select 
        doc_dt,
        po_num,
        sls_doc_num,
        sls_doc_item,
        sls_doc_type,
        bill_dt,
        bill_num,
        bill_item,
        doc_creation_dt,
        sold_to,
        matl_num,
        req_delvry_dt,
        rejectn_st,
        rejectn_cd,
        rejectn_desc,
        ord_qty,
        ord_net_price,
        ord_grs_trd_sls,
        ord_subtotal_2,
        ord_subtotal_3,
        ord_subtotal_4,
        ord_net_amt,
        ord_est_nts,
        bill_qty_pc,
        bill_grs_trd_sls,
        bill_subtotal_2,
        bill_subtotal_3,
        bill_subtotal_4,
        bill_net_amt,
        bill_est_nts,
        bill_net_val,
        bill_gross_val
    from transformed
)
select * from final