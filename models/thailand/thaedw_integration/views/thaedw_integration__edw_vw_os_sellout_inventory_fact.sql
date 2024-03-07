 with itg_th_sellout_inventory_fact as (
    select * from DEV_DNA_CORE.THAITG_INTEGRATION.ITG_TH_SELLOUT_INVENTORY_FACT
 ),
final as (
 SELECT
    'TH' AS cntry_cd,
    'Thailand' AS cntry_nm,
    f.wh_cd AS warehse_cd,
    f.dstrbtr_id AS dstrbtr_grp_cd,
    NULL  AS dstrbtr_soldto_code,
    f.prod_cd AS dstrbtr_matl_num,
    NULL  AS sap_matl_num,
    NULL  AS bar_cd,
    f.rec_dt AS inv_dt,
    (
      f.qty * CAST((
        CAST((
          12
        ) AS DECIMAL)
      ) AS DECIMAL(18, 0))
    ) AS soh,
    f.amt AS soh_val,
    f.amt AS jj_soh_val,
    0 AS beg_stock_qty,
    0 AS end_stock_qty,
    0 AS beg_stock_val,
    0 AS end_stock_val,
    0 AS jj_beg_stock_qty,
    0 AS jj_end_stock_qty,
    0 AS jj_beg_stock_val,
    0 AS jj_end_stock_val
  FROM itg_th_sellout_inventory_fact AS f
)
select * from final