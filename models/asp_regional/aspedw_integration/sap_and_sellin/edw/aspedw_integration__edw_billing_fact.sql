{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= [],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with itg_billing_fact as (
    select * from {{ ref('aspitg_integration__itg_billing_fact') }}
),

itg_crncy_mult as (
   select * from {{ source('aspitg_integration', 'itg_crncy_mult') }}
),
--Logical CTE

-- Final CTE
final as (
    select
  bill_num,
  bill_item,
  bill_dt,
  bill_type,
  sold_to,
  rt_promo,
  s_ord_item,
  doc_num,
  grs_wgt_dl,
  inv_qty,
  bill_qty,
  base_uom,
  exchg_rate,
  req_qty,
  sls_unit,
  payer,
  case
    when mul.currdec is null
    then rebate_bas
    else rebate_bas * power(10, (
      2 - currdec
    ))
  end as rebate_bas,
  no_inv_it,
  case
    when mul.currdec is null
    then subtotal_1
    else subtotal_1 * power(10, (
      2 - currdec
    ))
  end as subtotal_1,
  case
    when mul.currdec is null
    then subtotal_3
    else subtotal_3 * power(10, (
      2 - currdec
    ))
  end as subtotal_3,
  case
    when mul.currdec is null
    then subtotal_4
    else subtotal_4 * power(10, (
      2 - currdec
    ))
  end as subtotal_4,
  case
    when mul.currdec is null
    then subtotal_2
    else subtotal_2 * power(10, (
      2 - currdec
    ))
  end as subtotal_2,
  case
    when mul.currdec is null
    then netval_inv
    else netval_inv * power(10, (
      2 - currdec
    ))
  end as netval_inv,
  exchg_stat,
  zblqtycse,
  exratexacc,
  case
    when mul.currdec is null
    then subtotal_6
    else subtotal_6 * power(10, (
      2 - currdec
    ))
  end as subtotal_6,
  case
    when mul.currdec is null
    then gross_val
    else gross_val * power(10, (
      2 - currdec
    ))
  end as gross_val,
  unit_of_wt,
  case
    when mul.currdec is null
    then subtotal_5
    else subtotal_5 * power(10, (
      2 - currdec
    ))
  end as subtotal_5,
  numerator,
  case when mul.currdec is null then cost else cost * power(10, (
    2 - currdec
  )) end as cost,
  plant,
  volume_dl,
  loc_currcy,
  denomintr,
  volume_unit,
  scale_qty,
  case
    when mul.currdec is null
    then cshdsc_bas
    else cshdsc_bas * power(10, (
      2 - currdec
    ))
  end as cshdsc_bas,
  net_wgt_dl,
  case
    when mul.currdec is null
    then tax_amt
    else tax_amt * power(10, (
      2 - currdec
    ))
  end as tax_amt,
  rate_type,
  sls_org,
  exrate_acc,
  distr_chnl,
  doc_currcy,
  co_area,
  doc_categ,
  fisc_varnt,
  cost_center,
  matl_group,
  division,
  material,
  sls_grp,
  div_head,
  ship_point,
  wbs_elemt,
  bill_rule,
  bwapplnm,
  process_key,
  cust_grp,
  sls_off,
  refer_itm,
  matl_grp_3,
  price_dt,
  sls_emply,
  refer_doc,
  st_up_dte,
  stat_date,
  item_categ,
  prov_grp,
  matl_grp_5,
  prod_hier,
  itm_type,
  matl_grp_4,
  ship_to,
  bill_to_prty,
  rebate_grp,
  matl_grp_2,
  matl_grp_1,
  eanupc,
  mat_entrd,
  batch,
  stor_loc,
  created_on,
  serv_date,
  cust_grp5,
  sls_deal,
  bill_cat,
  cust_grp1,
  cust_grp3,
  trans_dt,
  cust_grp4,
  cust_grp2,
  stat_curr,
  ch_on,
  comp_cd,
  sls_dist,
  stor_no,
  record_mode,
  customer,
  cust_sls,
  oi_ebeln,
  oi_ebelp,
  zsd_pod,
  cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from itg_billing_fact as biil_ft, itg_crncy_mult as mul
where
  biil_ft.doc_currcy = mul.currkey)


--Final select
select * from final 