with itg_ims as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_IMS
),
itg_tw_ims_dstr_prod_map as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_TW_IMS_DSTR_PROD_MAP
),
itg_tw_ims_dstr_prod_price_map as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_TW_IMS_DSTR_PROD_PRICE_MAP
),
edw_material_sales_dim as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_MATERIAL_SALES_DIM
),
final as (
SELECT src.ims_txn_dt,
  src.dstr_cd,
  src.dstr_nm,
  src.cust_cd,
  src.cust_nm,
  src.prod_cd,
  src.prod_nm,
  src.rpt_per_strt_dt,
  src.rpt_per_end_dt,
  src.mod_ean_num AS ean_num,
  src.uom,
  src.unit_prc,
  src.sls_amt,
  src.sls_qty,
  src.rtrn_qty,
  src.rtrn_amt,
  src.ship_cust_nm,
  src.cust_cls_grp,
  src.cust_sub_cls,
  src.prod_spec,
  src.itm_agn_nm,
  src.ordr_co,
  src.rtrn_rsn,
  src.sls_ofc_cd,
  src.sls_grp_cd,
  src.sls_ofc_nm,
  src.sls_grp_nm,
  src.acc_type,
  src.co_cd,
  src.sls_rep_cd,
  src.sls_rep_nm,
  src.doc_dt,
  src.doc_num,
  src.invc_num,
  src.remark_desc,
  src.gift_qty,
  src.sls_bfr_tax_amt,
  src.sku_per_box,
  src.ctry_cd,
  src.crncy_cd,
  src.prom_sls_amt,
  src.prom_rtrn_amt,
  src.prom_prc_amt,
  convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS CRT_DTTM
FROM (
  SELECT x.*,
    CASE 
      WHEN x.ctry_cd = 'TW'
        AND lkp1.ean_cd IS NOT NULL
        THEN lkp1.ean_cd
      WHEN x.ctry_cd = 'HK'
        AND lkp2.ean_cd IS NOT NULL
        THEN lkp2.ean_cd
      ELSE COALESCE(x.ean_num, '#')
      END AS mod_ean_num,
    COALESCE((x.sls_qty * lkp3.sell_out_price_manual), 0) AS prom_sls_amt,
    COALESCE((x.rtrn_qty * lkp3.sell_out_price_manual), 0) AS prom_rtrn_amt,
    COALESCE(lkp3.sell_out_price_manual, CAST(0 AS NUMERIC(16, 5))) AS prom_prc_amt
  FROM (
    SELECT CASE 
        WHEN a.ims_txn_dt IS NOT NULL
          THEN a.ims_txn_dt
        ELSE b.ims_txn_dt
        END AS ims_txn_dt,
      CASE 
        WHEN a.dstr_cd IS NOT NULL
          THEN a.dstr_cd
        ELSE b.dstr_cd
        END AS dstr_cd,
      CASE 
        WHEN a.dstr_nm IS NOT NULL
          THEN a.dstr_nm
        ELSE b.dstr_nm
        END AS dstr_nm,
      CASE 
        WHEN a.cust_cd IS NOT NULL
          THEN a.cust_cd
        ELSE b.cust_cd
        END AS cust_cd,
      CASE 
        WHEN a.cust_nm IS NOT NULL
          THEN a.cust_nm
        ELSE b.cust_nm
        END AS cust_nm,
      CASE 
        WHEN a.prod_cd IS NOT NULL
          THEN a.prod_cd
        ELSE b.prod_cd
        END AS prod_cd,
      CASE 
        WHEN a.prod_nm IS NOT NULL
          THEN a.prod_nm
        ELSE b.prod_nm
        END AS prod_nm,
      CASE 
        WHEN a.rpt_per_strt_dt IS NOT NULL
          THEN a.rpt_per_strt_dt
        ELSE b.rpt_per_strt_dt
        END AS rpt_per_strt_dt,
      CASE 
        WHEN a.rpt_per_end_dt IS NOT NULL
          THEN a.rpt_per_end_dt
        ELSE b.rpt_per_end_dt
        END AS rpt_per_end_dt,
      CASE 
        WHEN a.ean_num IS NOT NULL
          THEN a.ean_num
        ELSE b.ean_num
        END AS ean_num,
      CASE 
        WHEN a.uom IS NOT NULL
          THEN a.uom
        ELSE b.uom
        END AS uom,
      CASE 
        WHEN a.unit_prc IS NOT NULL
          THEN a.unit_prc
        ELSE b.unit_prc
        END AS unit_prc,
      COALESCE(a.sls_amt, 0) AS sls_amt,
      COALESCE(a.sls_qty, 0) AS sls_qty,
      COALESCE(a.rtrn_qty, 0) + COALESCE(b.mod_sls_qty, 0) AS rtrn_qty,
      COALESCE(a.rtrn_amt, 0) + COALESCE(b.mod_sls_amt, 0) AS rtrn_amt,
      COALESCE(a.gift_qty, 0) + COALESCE(b.gift_qty, 0) AS gift_qty,
      COALESCE(a.sls_bfr_tax_amt, 0) + COALESCE(b.sls_bfr_tax_amt, 0) AS sls_bfr_tax_amt,
      CASE 
        WHEN a.ship_cust_nm IS NOT NULL
          THEN a.ship_cust_nm
        ELSE b.ship_cust_nm
        END AS ship_cust_nm,
      CASE 
        WHEN a.cust_cls_grp IS NOT NULL
          THEN a.cust_cls_grp
        ELSE b.cust_cls_grp
        END AS cust_cls_grp,
      CASE 
        WHEN a.cust_sub_cls IS NOT NULL
          THEN a.cust_sub_cls
        ELSE b.cust_sub_cls
        END AS cust_sub_cls,
      CASE 
        WHEN a.prod_spec IS NOT NULL
          THEN a.prod_spec
        ELSE b.prod_spec
        END AS prod_spec,
      CASE 
        WHEN a.itm_agn_nm IS NOT NULL
          THEN a.itm_agn_nm
        ELSE b.itm_agn_nm
        END AS itm_agn_nm,
      CASE 
        WHEN a.ordr_co IS NOT NULL
          THEN a.ordr_co
        ELSE b.ordr_co
        END AS ordr_co,
      CASE 
        WHEN a.rtrn_rsn IS NOT NULL
          THEN a.rtrn_rsn
        ELSE b.rtrn_rsn
        END AS rtrn_rsn,
      CASE 
        WHEN a.sls_ofc_cd IS NOT NULL
          THEN a.sls_ofc_cd
        ELSE b.sls_ofc_cd
        END AS sls_ofc_cd,
      CASE 
        WHEN a.sls_grp_cd IS NOT NULL
          THEN a.sls_grp_cd
        ELSE b.sls_grp_cd
        END AS sls_grp_cd,
      CASE 
        WHEN a.sls_ofc_nm IS NOT NULL
          THEN a.sls_ofc_nm
        ELSE b.sls_ofc_nm
        END AS sls_ofc_nm,
      CASE 
        WHEN a.sls_grp_nm IS NOT NULL
          THEN a.sls_grp_nm
        ELSE b.sls_grp_nm
        END AS sls_grp_nm,
      CASE 
        WHEN a.acc_type IS NOT NULL
          THEN a.acc_type
        ELSE b.acc_type
        END AS acc_type,
      CASE 
        WHEN a.co_cd IS NOT NULL
          THEN a.co_cd
        ELSE b.co_cd
        END AS co_cd,
      CASE 
        WHEN a.sls_rep_cd IS NOT NULL
          THEN a.sls_rep_cd
        ELSE b.sls_rep_cd
        END AS sls_rep_cd,
      CASE 
        WHEN a.sls_rep_nm IS NOT NULL
          THEN a.sls_rep_nm
        ELSE b.sls_rep_nm
        END AS sls_rep_nm,
      CASE 
        WHEN a.doc_dt IS NOT NULL
          THEN a.doc_dt
        ELSE b.doc_dt
        END AS doc_dt,
      CASE 
        WHEN a.doc_num IS NOT NULL
          THEN a.doc_num
        ELSE b.doc_num
        END AS doc_num,
      CASE 
        WHEN a.invc_num IS NOT NULL
          THEN a.invc_num
        ELSE b.invc_num
        END AS invc_num,
      CASE 
        WHEN a.remark_desc IS NOT NULL
          THEN a.remark_desc
        ELSE b.remark_desc
        END AS remark_desc,
      CASE 
        WHEN a.sku_per_box IS NOT NULL
          THEN a.sku_per_box
        ELSE b.sku_per_box
        END AS sku_per_box,
      CASE 
        WHEN a.ctry_cd IS NOT NULL
          THEN a.ctry_cd
        ELSE b.ctry_cd
        END AS ctry_cd,
      CASE 
        WHEN a.crncy_cd IS NOT NULL
          THEN a.crncy_cd
        ELSE b.crncy_cd
        END AS crncy_cd
    FROM (
      SELECT *
      FROM itg_ims
      WHERE doc_type <> 'Return'
        OR doc_type IS NULL
        OR doc_type = ''
      ) a
    FULL OUTER JOIN (
      SELECT m.*,
        CASE 
          WHEN m.sls_amt < 0
            THEN m.sls_amt * - 1
          ELSE m.sls_amt
          END AS mod_sls_amt,
        CASE 
          WHEN m.sls_qty < 0
            THEN m.sls_qty * - 1
          ELSE m.sls_qty
          END AS mod_sls_qty
      FROM itg_ims m
      WHERE m.doc_type = 'Return'
      ) b ON a.dstr_cd = b.dstr_cd
      AND a.cust_cd = b.cust_cd
      AND a.prod_cd = b.prod_cd
      AND a.prod_nm = b.prod_nm
      AND a.ims_txn_dt = b.ims_txn_dt
      AND a.cust_nm = b.cust_nm
      AND a.ean_num = b.ean_num
    ) x
  LEFT JOIN itg_tw_ims_dstr_prod_map lkp1 ON x.dstr_cd = lkp1.dstr_cd
    AND (
      x.prod_cd = lkp1.dstr_prod_cd
      OR x.prod_nm = lkp1.dstr_prod_nm
      )
  LEFT JOIN itg_tw_ims_dstr_prod_price_map lkp3 ON x.dstr_cd = lkp3.dstr_cd
    AND (x.prod_cd = lkp3.dstr_prod_cd)
  LEFT JOIN (
    SELECT MAX(ean_num) AS ean_cd,
      matl_num
    FROM (
      SELECT DISTINCT LTRIM(REPLACE(REPLACE(ean_num, ' ', ''), '-', ''), 0) AS ean_num,
        LTRIM(matl_num, 0) AS matl_num
      FROM edw_material_sales_dim
      WHERE ean_num IS NOT NULL
        AND ean_num NOT IN ('', 'NA', 'TBC', 'N/A', 'NIL', 'TBA', '#N/A', 'NOT APPLICABLE', 'NIA', 'N/AA', 'N./A', 'MA', '0')
        AND sls_org IN ('1110', '110S')
        AND dstr_chnl = 10
      )
    GROUP BY matl_num
    ) lkp2 ON LTRIM(x.prod_cd, 0) = lkp2.matl_num
  ) src
WHERE --src.dstr_cd in  ('110238','100496','119671','100681','110256','107479','107481','107482','107490','107483','107485','107507','107510','115973','116047','122296','132222','132349')
  src.dstr_cd IN ('100681', '110256')
  AND upper(trim(src.ctry_cd)) = 'HK'
)

select * from final 

