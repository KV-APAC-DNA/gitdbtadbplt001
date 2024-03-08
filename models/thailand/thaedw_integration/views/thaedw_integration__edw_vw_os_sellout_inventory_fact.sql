 with itg_th_sellout_inventory_fact as (
    select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_SELLOUT_INVENTORY_FACT
 ),
final as (
 select
    'TH' AS cntry_cd,
    'Thailand' AS cntry_nm,
    f.wh_cd as warehse_cd,
    f.dstrbtr_id as dstrbtr_grp_cd,
    null  as dstrbtr_soldto_code,
    f.prod_cd as dstrbtr_matl_num,
    null  as sap_matl_num,
    null  as bar_cd,
    f.rec_dt as inv_dt,
    (
      f.qty * cast((
        cast((
          12
        ) as decimal)
      ) as decimal(18, 0))
    ) as soh,
    f.amt as soh_val,
    f.amt as jj_soh_val,
    0 as beg_stock_qty,
    0 as end_stock_qty,
    0 as beg_stock_val,
    0 as end_stock_val,
    0 as jj_beg_stock_qty,
    0 as jj_end_stock_qty,
    0 as jj_beg_stock_val,
    0 as jj_end_stock_val
  from itg_th_sellout_inventory_fact as f
)
select * from final