{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["file_name"],
        post_hook="{{sap_transaction_processed_files('BWA_CDL_BILLING','vw_stg_sdl_sap_bw_billing','itg_billing_fact')}}"
        )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_billing') }}
),
sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
--Logical CTE

--Final CTE
final as (
  select
  bill_num::varchar(50) as bill_num,
  bill_item::varchar(50) as bill_item,
  try_to_date(bill_dt, 'yyyymmdd') as bill_dt,
  bill_type::varchar(30) as bill_type,
  sold_to::varchar(50) as sold_to,
  rt_promo::varchar(100) as rt_promo,
  s_ord_item::varchar(50) as s_ord_item,
  doc_num::varchar(50) as doc_num,
  cast(grs_wgt_dl as decimal(20, 4)) as grs_wgt_dl,
  cast(inv_qty as decimal(20, 4)) as inv_qty,
  cast(bill_qty as decimal(20, 4)) as bill_qty,
  base_uom::varchar(30) as base_uom,
  cast(exchg_rate as decimal(20, 4)) as exchg_rate,
  cast(req_qty as decimal(20, 4)) as req_qty,
  sls_unit::varchar(20) as sls_unit,
  payer::varchar(50) as payer,
  cast(rebate_bas as decimal(20, 4)) as rebate_bas,
  cast(no_inv_it as decimal(20, 4)) as no_inv_it,
  cast(subtotal_1 as decimal(20, 4)) as subtotal_1,
  cast(subtotal_3 as decimal(20, 4)) as subtotal_3,
  cast(subtotal_4 as decimal(20, 4)) as subtotal_4,
  cast(subtotal_2 as decimal(20, 4)) as subtotal_2,
  cast(netval_inv as decimal(20, 4)) as netval_inv,
  cast(exchg_stat as decimal(20, 4)) as exchg_stat,
  cast(zblqtycse as decimal(20, 4)) as zblqtycse,
  cast(exratexacc as decimal(20, 4)) as exratexacc,
  cast(subtotal_6 as decimal(20, 4)) as subtotal_6,
  cast(gross_val as decimal(20, 4)) as gross_val,
  unit_of_wt::varchar(20) as unit_of_wt,
  cast(subtotal_5 as decimal(20, 4)) as subtotal_5,
  cast(replace(numerator, ',', '') as decimal(20, 4)) as numerator,
  cast(cost as decimal(20, 4)) as cost,
  plant::varchar(30) as plant,
  cast(volume_dl as decimal(20, 4)) as volume_dl,
  loc_currcy::varchar(30) as loc_currcy,
  cast(replace(denomintr,',','') as decimal(20, 4)) as denomintr,
  volume_unit::varchar(20) as volume_unit,
  scale_qty::varchar(20)  as scale_qty,
  cast(cshdsc_bas as decimal(20, 4)) as cshdsc_bas,
  cast(net_wgt_dl as decimal(20, 4)) as net_wgt_dl,
  cast(tax_amt as decimal(20, 4)) as tax_amt,
  rate_type::varchar(30) as rate_type,
  sls_org::varchar(20) as sls_org,
  cast(exrate_acc as decimal(20, 4)) as exrate_acc,
  distr_chnl::varchar(30) as distr_chnl,
  doc_currcy::varchar(30) as doc_currcy,
  co_area::varchar(30) as co_area,
  doc_categ::varchar(20) as doc_categ,
  fisc_varnt::varchar(20) as fisc_varnt,
  cost_center::varchar(100) as cost_center,
  matl_group::varchar(30) as matl_group,
  division::varchar(30) as division,
  material::varchar(50) as material,
  sls_grp::varchar(30) as sls_grp,
  div_head::varchar(100) as div_head,
  ship_point::varchar(50) as ship_point,
  wbs_elemt::varchar(100) as wbs_elemt ,
  bill_rule::varchar(100) as bill_rule,
  bwapplnm::varchar(30) as bwapplnm,
  process_key::varchar(50) as process_key,
  cust_grp::varchar(50) as cust_grp,
  sls_off::varchar(50) as sls_off,
  refer_itm::varchar(50) as refer_itm,
  matl_grp_3::varchar(50) as matl_grp_3,
  try_to_date(price_dt, 'yyyymmdd') as price_dt,
  sls_emply::varchar(100) as sls_emply,
  refer_doc::varchar(100) as refer_doc,
  try_to_date(st_up_dte, 'yyyymmdd') as st_up_dte,
  stat_date::varchar(100) as stat_date,
  item_categ::varchar(100) as item_categ,
  prov_grp::varchar(50) as prov_grp,
  matl_grp_5::varchar(50) as matl_grp_5,
  prod_hier::varchar(100) as prod_hier,
  itm_type::varchar(50) as itm_type,
  matl_grp_4::varchar(30) as matl_grp_4,
  ship_to::varchar(50) as ship_to,
  bill_to_prty::varchar(50) as bill_to_prty,
  rebate_grp::varchar(50) as rebate_grp,
  matl_grp_2::varchar(30) as matl_grp_2,
  matl_grp_1::varchar(30) as matl_grp_1,
  eanupc::varchar(50) as eanupc,
  mat_entrd::varchar(50) as mat_entrd,
  batch::varchar(100) as batch,
  stor_loc::varchar(50) as stor_loc,
  try_to_date(created_on, 'yyyymmdd') as created_on,
  try_to_date(serv_date, 'yyyymmdd') as serv_date,
  cust_grp5::varchar(100) as cust_grp5,
  sls_deal::varchar(100) as sls_deal,
  bill_cat::varchar(100) as bill_cat,
  cust_grp1::varchar(100) as cust_grp1,
  cust_grp3::varchar(100)as cust_grp3,
  try_to_date(trans_dt, 'yyyymmdd') as trans_dt,
  cust_grp4::varchar(100) as cust_grp4,
  cust_grp2::varchar(100) as cust_grp2,
  stat_curr::varchar(50) as stat_curr,
  ch_on::varchar(50) as ch_on,
  comp_cd::varchar(50) as comp_cd,
  sls_dist::varchar(50) as sls_dist,
  stor_no::varchar(100) as stor_no,
  record_mode::varchar(20) as record_mode,
  customer::varchar(50) as customer,
  cust_sls::varchar(50) as cust_sls,
  oi_ebeln::varchar(100) as oi_ebeln,
  oi_ebelp::varchar(100) as oi_ebelp,
  zsd_pod::varchar(100) as zsd_pod,
  cdl_dttm::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name::varchar(255) as file_name
  from source
  where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='itg_billing_fact' and sap_transactional_processed_files.act_file_name=source.file_name
  )
)

--final select
select * from final 