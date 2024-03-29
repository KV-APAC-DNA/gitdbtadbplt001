{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["file_name"],
        post_hook="{{sap_transaction_processed_files('BWA_CDL_SALES','vw_stg_sdl_sap_bw_sales','itg_sales_order_fact')}}"
        )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_sales') }}
),
sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
--Logical CTE

--Final CTE
final as (
select
  doc_num::varchar(50) as doc_num,
  s_ord_item::varchar(50) as s_ord_item,
  rejectn_st::varchar(20) as rejectn_st,
  process_key::varchar(20) as process_key,
  batch::varchar(20) as batch,
  try_to_date(created_on, 'yyyymmdd') as created_on,
  sls_deal::varchar(100) as sls_deal,
  stor_loc::varchar(20) as stor_loc,
  matl_grp::varchar(20) as matl_grp,
  incoterms2::varchar(50) as incoterms2,
  accnt_asgn::varchar(20) as accnt_asgn,
  cust_grp::varchar(20) as cust_grp,
  cond_pr_un::varchar(20) as cond_pr_un,
  incoterms::varchar(20) as incoterms,
  cast(replace(subtotal_4, ',', '') as decimal(20, 4)) as subtotal_4,
  cast(replace(subtotal_5, ',', '') as decimal(20, 4)) as subtotal_5,
  cast(replace(net_price, ',', '') as decimal(20, 4)) as net_price,
  cast(replace(net_value, ',', '') as decimal(20, 4)) as net_value,
  cast(replace(net_wt_ap, ',', '') as decimal(20, 4)) as net_wt_ap,
  cond_unit::varchar(20) as cond_unit,
  unit_of_wt::varchar(20) as unit_of_wt,
  order_curr::varchar(20) as order_curr,
  stat_curr::varchar(20) as stat_curr,
  loc_currcy::varchar(20) as loc_currcy,
  cast(replace(target_qty, ',', '') as decimal(20, 4)) as target_qty,
  cast(replace(targ_value, ',', '') as decimal(20, 4)) as targ_value,
  cast(replace(ord_items, ',', '') as decimal(20, 4)) as ord_items,
  cast(replace(zorqtycse, ',', '') as decimal(20, 4)) as zorqtycse,
  cast(replace(tax_value, ',', '') as decimal(20, 4)) as tax_value,
  cast(replace(exchg_rate, ',', '') as decimal(20, 4)) as exchg_rate,
  target_qu::varchar(30) as target_qu,
  cast(replace(cost, ',', '') as decimal(20, 4)) as cost,
  cast(replace(volume_ap, ',', '') as decimal(20, 4)) as volume_ap,
  volume_unit::varchar(30) as volume_unit,
  cast(replace(exchg_stat, ',', '') as decimal(20, 4)) as exchg_stat,
  cast(replace(reqdel_qty, ',', '') as decimal(20, 4)) as reqdel_qty,
  cast(replace(lowr_bnd, ',', '') as decimal(20, 4)) as lowr_bnd,
  cast(replace(uppr_bnd, ',', '') as decimal(20, 4)) as uppr_bnd,
  cast(replace(numeratorz, ',', '') as decimal(20, 4)) as numeratorz,
  cast(replace(denomintrz, ',', '') as decimal(20, 4)) as denomintrz,
  cast(replace(numerator, ',', '') as decimal(20, 4)) as numerator,
  cast(replace(denomintr, ',', '') as decimal(20, 4)) as denomintr,
  min_dl_qty::varchar(30) as min_dl_qty,
  cast(replace(subtotal_6, ',', '') as decimal(20, 4)) as subtotal_6,
  cast(replace(subtotal_3, ',', '') as decimal(20, 4)) as subtotal_3,
  wbs_elemt::varchar(100) as wbs_elemt,
  refer_doc::varchar(30) as refer_doc,
  cast(replace(subtotal_2, ',', '') as decimal(20, 4)) as subtotal_2,
  cast(replace(subtotal_1, ',', '') as decimal(20, 4)) as subtotal_1,
  doc_currcy::varchar(30) as doc_currcy,
  cast(replace(cml_or_qty, ',', '') as decimal(20, 4)) as cml_or_qty,
  cast(replace(cml_cd_qty, ',', '') as decimal(20, 4)) as cml_cd_qty,
  base_uom::varchar(30) as base_uom,
  material::varchar(50) as material,
  refer_itm::varchar(30) as refer_itm,
  fisc_varnt::varchar(30) as fisc_varnt,
  apo_planned::varchar(30) as apo_planned,
  log_sys::varchar(30) as log_sys,
  cast(replace(cml_cf_qty, ',', '') as decimal(20, 4)) as cml_cf_qty,
  sls_unit::varchar(30) as sls_unit,
  try_to_date(doc_dt, 'yyyymmdd') as doc_dt,
  try_to_date(zreq_dt, 'yyyymmdd') as zreq_dt,
  zord_mth::varchar(30) as zord_mth,
  sub_reason::varchar(30) as sub_reason,
  doc_categr::varchar(30) as doc_categr,
  div_head::varchar(30) as div_head,
  try_to_date(trans_dt, 'yyyymmdd') as trans_dt,
  try_to_date(bill_dt, 'yyyymmdd') as bill_dt,
  prod_cat::varchar(100) as prod_cat,
  cast(replace(exchg_crd, ',', '') as decimal(20, 4)) as exchg_crd,
  sales_dist::varchar(30) as sales_dist,
  serv_dt::varchar(30) as serv_dt,
  plant::varchar(30) as plant,
  rt_promo::varchar(100) as rt_promo,
  mat_entrd::varchar(50) as mat_entrd,
  ship_point::varchar(30) as ship_point,
  prvdoc_ctg::varchar(30) as prvdoc_ctg,
  crea_time::varchar(30) as crea_time,
  bilblk_itm::varchar(100) as bilblk_itm,
  matl_grp_1::varchar(30) as  matl_grp_1,
  matl_grp_3::varchar(30) as matl_grp_3,
  price_dt::varchar(30) as price_dt,
  matl_grp_4::varchar(30) as matl_grp_4,
  matl_grp_5::varchar(30) as matl_grp_5,
  route::varchar(30) as route,
  sls_emply::varchar(30) as sls_emply,
  bnd_ind::varchar(30) as bnd_ind,
  cast(replace(gross_wgt, ',', '') as decimal(20, 4)) as gross_wgt,
  stat_dt::varchar(30) as stat_dt,
  division::varchar(30) as division,
  try_to_date(st_up_dt, 'yyyymmdd') as st_up_dt,
  ship_stck::varchar(100) as ship_stck,
  forwagent::varchar(100) as forwagent,
  bill_to_prty::varchar(30) as bill_to_prty,
  prod_hier::varchar(30) as prod_hier,
  item_categ::varchar(30) as item_categ,
  ship_to::varchar(30) as ship_to,
  payer::varchar(30) as payer,
  unld_pt_we::varchar(100) as unld_pt_we,
  matl_grp_2::varchar(30) as matl_grp_2,
  eanupc::varchar(30) as eanupc,
  created_by::varchar(30) as created_by,
  sts_bill::varchar(30) as sts_bill,
  order_prob::varchar(30) as order_prob,
  bwapplnm::varchar(30) as bwapplnm,
  quot_from::varchar(30) as quot_from,
  ord_reason::varchar(30) as ord_reason,
  ch_on::varchar(30) as ch_on,
  reason_rej::varchar(30) as reason_rej ,
  comp_cd::varchar(30) as comp_cd,
  distr_chan::varchar(30) as distr_chan,
  sls_org::varchar(30) as sls_org,
  sls_grp::varchar(30) as sls_grp,
  sls_off::varchar(30) as sls_off,
  doc_categ::varchar(30) as doc_categ,
  rate_type::varchar(30) as rate_type,
  bill_block::varchar(30) as bill_block,
  cust_grp1::varchar(100) as cust_grp1,
  cust_grp2::varchar(100) as cust_grp2,
  cust_grp4::varchar(100) as cust_grp4,
  cust_grp3::varchar(30) as cust_grp3,
  cust_grp5::varchar(100) as cust_grp5,
  del_block::varchar(30) as del_block,
  sold_to::varchar(30) as sold_to,
  quot_to::varchar(30) as quot_to,
  doc_type::varchar(30) as doc_type,
  sts_prc::varchar(30) as sts_prc,
  sts_del::varchar(30) as sts_del,
  sts_itm::varchar(30) as sts_itm,
  stor_no::varchar(100) as stor_no,
  record_mode::varchar(30) as record_mode,
  cast(replace(zordqtybu, ',', '') as decimal(20, 4)) as zordqtybu,
  zzp_num::varchar(30) as zzp_num,
  zzp_itm::varchar(30) as zzp_itm,
  ztarqtycu::varchar(100) as ztarqtycu,
  ztarqtybu::varchar(100) as ztarqtybu,
  zhighritm::varchar(30) as zhighritm,
  cdl_dttm::varchar(50) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name::varchar(255) as file_name
  from source
  where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='itg_sales_order_fact' and sap_transactional_processed_files.act_file_name=source.file_name
  )
)

--Final select
select * from final 