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
  bill_num::varchar(50) as bill_num,
bill_item::varchar(50) as bill_item,
bill_dt::date as bill_dt,
bill_type::varchar(30) as bill_type,
sold_to::varchar(50) as sold_to,
rt_promo::varchar(100) as rt_promo,
s_ord_item::varchar(50) as s_ord_item,
doc_num::varchar(50) as doc_num,
grs_wgt_dl::number(20,4) as grs_wgt_dl,
inv_qty::number(20,4) as inv_qty,
bill_qty::number(20,4) as bill_qty,
base_uom::varchar(30) as base_uom,
exchg_rate::number(20,4) as exchg_rate,
req_qty::number(20,4) as req_qty,
sls_unit::varchar(20) as sls_unit,
payer::varchar(50) as payer,

  case
    when mul.currdec is null
    then rebate_bas
    else rebate_bas * power(10, (
      2 - currdec
    ))
  end::number(20,4) as rebate_bas,
  no_inv_it,
  case
    when mul.currdec is null
    then subtotal_1
    else subtotal_1 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_1,
  case
    when mul.currdec is null
    then subtotal_3
    else subtotal_3 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_3,
  case
    when mul.currdec is null
    then subtotal_4
    else subtotal_4 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_4,
  case
    when mul.currdec is null
    then subtotal_2
    else subtotal_2 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_2,
  case
    when mul.currdec is null
    then netval_inv
    else netval_inv * power(10, (
      2 - currdec
    ))
  end::number(20,4) as netval_inv,
  exchg_stat,
  zblqtycse,
  exratexacc,
  case
    when mul.currdec is null
    then subtotal_6
    else subtotal_6 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_6,
  case
    when mul.currdec is null
    then gross_val
    else gross_val * power(10, (
      2 - currdec
    ))
  end::number(20,4) as gross_val,
  unit_of_wt::varchar(20) as unit_of_wt,
  case
    when mul.currdec is null
    then subtotal_5
    else subtotal_5 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_5,
  numerator,
  case when mul.currdec is null then cost else cost * power(10, (
    2 - currdec
  )) end::number(20,4) as cost,
  plant::VARCHAR(30) as plant,
  volume_dl::number(20,4) as volume_dl,
  loc_currcy::VARCHAR(30) as loc_currcy,
  denomintr::number(20,4) as denomintr,
  volume_unit::VARCHAR(20) as volume_unit,
  scale_qty::VARCHAR(20) scale_qty,
  case
    when mul.currdec is null
    then cshdsc_bas
    else cshdsc_bas * power(10, (
      2 - currdec
    ))
  end::number(20,4) as cshdsc_bas,
  net_wgt_dl,
  case
    when mul.currdec is null
    then tax_amt
    else tax_amt * power(10, (
      2 - currdec
    ))
  end::number(20,4) as tax_amt,
  rate_type::varchar(30) as rate_type,
sls_org::varchar(20) as sls_org,
exrate_acc::number(20,4) as exrate_acc,
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
wbs_elemt::varchar(100) as wbs_elemt,
bill_rule::varchar(100) as bill_rule,
bwapplnm::varchar(30) as bwapplnm,
process_key::varchar(50) as process_key,
cust_grp::varchar(50) as cust_grp,
sls_off::varchar(50) as sls_off,
refer_itm::varchar(50) as refer_itm,
matl_grp_3::varchar(50) as matl_grp_3,
price_dt::date as price_dt,
sls_emply::varchar(100) as sls_emply,
refer_doc::varchar(100) as refer_doc,
st_up_dte::date as st_up_dte,
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
created_on::date as created_on,
serv_date::date as serv_date,
cust_grp5::varchar(100) as cust_grp5,
sls_deal::varchar(100) as sls_deal,
bill_cat::varchar(100) as bill_cat,
cust_grp1::varchar(100) as cust_grp1,
cust_grp3::varchar(100) as cust_grp3,
trans_dt::date as trans_dt,
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
current_timestamp()::timestamp_ntz(9) as updt_dttm
from itg_billing_fact as biil_ft, itg_crncy_mult as mul
where
  biil_ft.doc_currcy = mul.currkey)


--Final select
select * from final 