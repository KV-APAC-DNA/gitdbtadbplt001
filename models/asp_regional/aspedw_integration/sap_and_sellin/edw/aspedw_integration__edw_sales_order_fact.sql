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
with itg_sales_order_fact as (
    select * from {{ ref('aspitg_integration__itg_sales_order_fact') }}
),

itg_crncy_mult as (
   select * from {{ source('aspitg_integration', 'itg_crncy_mult') }}
),
--Logical CTE

--Final CTE
final as (
    select
  doc_num,
  s_ord_item,
  rejectn_st,
  process_key,
  batch,
  created_on,
  sls_deal,
  stor_loc,
  matl_grp,
  incoterms2,
  accnt_asgn,
  cust_grp,
  cond_pr_un,
  incoterms,
  case
    when mul.currdec is null
    then subtotal_4
    else subtotal_4 * power(10, (
      2 - currdec
    ))
  end as subtotal_4,
  case
    when mul.currdec is null
    then subtotal_5
    else subtotal_5 * power(10, (
      2 - currdec
    ))
  end as subtotal_5,
  case
    when mul.currdec is null
    then net_price
    else net_price * power(10, (
      2 - currdec
    ))
  end as net_price,
  case
    when mul.currdec is null
    then net_value
    else net_value * power(10, (
      2 - currdec
    ))
  end as net_value,
  net_wt_ap,
  cond_unit,
  unit_of_wt,
  order_curr,
  stat_curr,
  loc_currcy,
  target_qty,
  case
    when mul.currdec is null
    then targ_value
    else targ_value * power(10, (
      2 - currdec
    ))
  end as targ_value,
  ord_items,
  zorqtycse,
  case
    when mul.currdec is null
    then tax_value
    else tax_value * power(10, (
      2 - currdec
    ))
  end as tax_value,
  exchg_rate,
  target_qu,
  case when mul.currdec is null then cost else cost * power(10, (
    2 - currdec
  )) end as cost,
  volume_ap,
  volume_unit,
  exchg_stat,
  reqdel_qty,
  lowr_bnd,
  uppr_bnd,
  numeratorz,
  denomintrz,
  numerator,
  denomintr,
  min_dl_qty,
  case
    when mul.currdec is null
    then subtotal_6
    else subtotal_6 * power(10, (
      2 - currdec
    ))
  end as subtotal_6,
  case
    when mul.currdec is null
    then subtotal_3
    else subtotal_3 * power(10, (
      2 - currdec
    ))
  end as subtotal_3,
  wbs_elemt,
  refer_doc,
  case
    when mul.currdec is null
    then subtotal_2
    else subtotal_2 * power(10, (
      2 - currdec
    ))
  end as subtotal_2,
  case
    when mul.currdec is null
    then subtotal_1
    else subtotal_1 * power(10, (
      2 - currdec
    ))
  end as subtotal_1,
  doc_currcy,
  cml_or_qty,
  cml_cd_qty,
  base_uom,
  material,
  refer_itm,
  fisc_varnt,
  apo_planned,
  log_sys,
  cml_cf_qty,
  sls_unit,
  doc_dt,
  zreq_dt,
  zord_mth,
  sub_reason,
  doc_categr,
  div_head,
  trans_dt,
  bill_dt,
  prod_cat,
  exchg_crd,
  sales_dist,
  serv_dt,
  plant,
  rt_promo,
  mat_entrd,
  ship_point,
  prvdoc_ctg,
  crea_time,
  bilblk_itm,
  matl_grp_1,
  matl_grp_3,
  price_dt,
  matl_grp_4,
  matl_grp_5,
  route,
  sls_emply,
  bnd_ind,
  gross_wgt,
  stat_dt,
  division,
  st_up_dt,
  ship_stck,
  forwagent,
  bill_to_prty,
  prod_hier,
  item_categ,
  ship_to,
  payer,
  unld_pt_we,
  matl_grp_2,
  eanupc,
  created_by,
  sts_bill,
  order_prob,
  bwapplnm,
  quot_from,
  ord_reason,
  ch_on,
  reason_rej,
  comp_cd,
  distr_chan,
  sls_org,
  sls_grp,
  sls_off,
  doc_categ,
  rate_type,
  bill_block,
  cust_grp1,
  cust_grp2,
  cust_grp4,
  cust_grp3,
  cust_grp5,
  del_block,
  sold_to,
  quot_to,
  doc_type,
  sts_prc,
  sts_del,
  sts_itm,
  stor_no,
  record_mode,
  zordqtybu,
  zzp_num,
  zzp_itm,
  ztarqtycu,
  ztarqtybu,
  zhighritm,
  cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from itg_sales_order_fact as so_ft, itg_crncy_mult as mul
where
  so_ft.doc_currcy = mul.currkey
)


--Final select
select * from final 