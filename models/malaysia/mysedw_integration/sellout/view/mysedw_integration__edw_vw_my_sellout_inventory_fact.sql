with itg_my_sellout_stock_fact as
(
    select * from {{ ref('mysitg_integration__itg_my_sellout_stock_fact') }}
),
itg_my_customer_dim as 
(
    select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
final as (select
  'MY' as cntry_cd,
  'Malaysia' as cntry_nm,
  imdsf.dstrbtr_wh_id as warehse_cd,
  imcd.dstrbtr_grp_cd,
  imdsf.cust_id as dstrbtr_soldto_code,
  imdsf.dstrbtr_prod_cd as dstrbtr_matl_num,
  imdsf.sap_matl_num,
  imdsf.ean_num as bar_cd,
  imdsf.inv_dt,
  imdsf.available_qty_pc as soh,
  imdsf.total_val as soh_val,
  0 as jj_soh_val,
  0 as beg_stock_qty,
  imdsf.available_qty_pc as end_stock_qty,
  0 as beg_stock_val,
  imdsf.total_val as end_stock_val,
  0 as jj_beg_stock_qty,
  0 as jj_end_stock_qty,
  0 as jj_beg_stock_val,
  0 as jj_end_stock_val
from (
    itg_my_sellout_stock_fact as imdsf
    left join itg_my_customer_dim as imcd
      on (
        (
          ltrim(cast((
            imcd.cust_id
          ) as text), cast((
            cast('0' as varchar)
          ) as text)) = ltrim(cast((
            imdsf.cust_id
          ) as text), cast((
            cast('0' as varchar)
          ) as text))
        )
      )
)
)
select * from final