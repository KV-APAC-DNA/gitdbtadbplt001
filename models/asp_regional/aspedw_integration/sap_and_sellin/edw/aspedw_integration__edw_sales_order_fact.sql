
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
doc_num::varchar(50) as doc_num,
s_ord_item::varchar(50) as s_ord_item,
rejectn_st::varchar(20) as rejectn_st,
process_key::varchar(20) as process_key,
batch::varchar(20) as batch,
created_on::date as created_on,
sls_deal::varchar(100) as sls_deal,
stor_loc::varchar(20) as stor_loc,
matl_grp::varchar(20) as matl_grp,
incoterms2::varchar(50) as incoterms2,
accnt_asgn::varchar(20) as accnt_asgn,
cust_grp::varchar(20) as cust_grp,
cond_pr_un::varchar(20) as cond_pr_un,
incoterms::varchar(20) as incoterms,
  case
    when mul.currdec is null
    then subtotal_4
    else subtotal_4 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_4,
  case
    when mul.currdec is null
    then subtotal_5
    else subtotal_5 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_5,
  case
    when mul.currdec is null
    then net_price
    else net_price * power(10, (
      2 - currdec
    ))
  end::number(20,4) as net_price,
  case
    when mul.currdec is null
    then net_value
    else net_value * power(10, (
      2 - currdec
    ))
  end::number(20,4) as net_value,
  net_wt_ap,
  cond_unit::varchar(20) as cond_unit,
  unit_of_wt::varchar(20) as unit_of_wt,
  order_curr::varchar(20) as order_curr,
  stat_curr::varchar(20) as stat_curr,
  loc_currcy::varchar(20) as loc_currcy,
  target_qty::number(20,4) as target_qty,
  case
    when mul.currdec is null
    then targ_value
    else targ_value * power(10, (
      2 - currdec
    ))
  end as targ_value,
  ord_items::number(20,4) as ord_items,
  zorqtycse::number(20,4) as zorqtycse,
  case
    when mul.currdec is null
    then tax_value
    else tax_value * power(10, (
      2 - currdec
    ))
  end as tax_value,
  exchg_rate::number(20,4) as exchg_rate,
  target_qu::varchar(30) as target_qu,
  case when mul.currdec is null then cost else cost * power(10, (
    2 - currdec
  )) end::number(20,4) as cost,
  volume_ap::number(20,4) as volume_ap,
  volume_unit::varchar(30) as volume_unit,
  exchg_stat::number(20,4) as exchg_stat,
  reqdel_qty::number(20,4) as reqdel_qty,
  lowr_bnd::number(20,4) as lowr_bnd,
  uppr_bnd::number(20,4) as uppr_bnd,
  numeratorz::number(20,4) as numeratorz,
  denomintrz::number(20,4) as denomintrz,
  numerator::number(20,4) as numerator,
  denomintr::number(20,4) as denomintr,
  min_dl_qty::varchar(30) as min_dl_qty,
  case
    when mul.currdec is null
    then subtotal_6
    else subtotal_6 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_6,
  case
    when mul.currdec is null
    then subtotal_3
    else subtotal_3 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_3,
  wbs_elemt::varchar(100) as wbs_elemt,
  refer_doc::varchar(30) as refer_doc,
  case
    when mul.currdec is null
    then subtotal_2
    else subtotal_2 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_2,
  case
    when mul.currdec is null
    then subtotal_1
    else subtotal_1 * power(10, (
      2 - currdec
    ))
  end::number(20,4) as subtotal_1,
doc_currcy::varchar(30) as doc_currcy,
cml_or_qty::number(20,4) as cml_or_qty,
cml_cd_qty::number(20,4) as cml_cd_qty,
base_uom::varchar(30) as base_uom,
material::varchar(50) as material,
refer_itm::varchar(30) as refer_itm,
fisc_varnt::varchar(30) as fisc_varnt,
apo_planned::varchar(30) as apo_planned,
log_sys::varchar(30) as log_sys,
cml_cf_qty::number(20,4) as cml_cf_qty,
sls_unit::varchar(30) as sls_unit,
doc_dt::date as doc_dt,
zreq_dt::date as zreq_dt,
zord_mth::varchar(30) as zord_mth,
sub_reason::varchar(30) as sub_reason,
doc_categr::varchar(30) as doc_categr,
div_head::varchar(30) as div_head,
trans_dt::date as trans_dt,
bill_dt::date as bill_dt,
prod_cat::varchar(100) as prod_cat,
exchg_crd::number(20,4) as exchg_crd,
sales_dist::varchar(30) as sales_dist,
serv_dt::varchar(30) as serv_dt,
plant::varchar(30) as plant,
rt_promo::varchar(100) as rt_promo,
mat_entrd::varchar(50) as mat_entrd,
ship_point::varchar(30) as ship_point,
prvdoc_ctg::varchar(30) as prvdoc_ctg,
crea_time::varchar(30) as crea_time,
bilblk_itm::varchar(100) as bilblk_itm,
matl_grp_1::varchar(30) as matl_grp_1,
matl_grp_3::varchar(30) as matl_grp_3,
price_dt::varchar(30) as price_dt,
matl_grp_4::varchar(30) as matl_grp_4,
matl_grp_5::varchar(30) as matl_grp_5,
route::varchar(30) as route,
sls_emply::varchar(30) as sls_emply,
bnd_ind::varchar(100) as bnd_ind,
gross_wgt::number(20,4) as gross_wgt,
stat_dt::varchar(30) as stat_dt,
division::varchar(30) as division,
st_up_dt::date as st_up_dt,
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
reason_rej::varchar(30) as reason_rej,
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
zordqtybu::number(20,4) as zordqtybu,
zzp_num::varchar(30) as zzp_num,
zzp_itm::varchar(30) as zzp_itm,
ztarqtycu::varchar(100) as ztarqtycu,
ztarqtybu::varchar(100) as ztarqtybu,
zhighritm::varchar(30) as zhighritm,
cdl_dttm::varchar(50) as cdl_dttm,
current_timestamp()::timestamp_ntz(9) as crtd_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
from itg_sales_order_fact as so_ft, itg_crncy_mult as mul
where
  so_ft.doc_currcy = mul.currkey(+)
)


--Final select
select * from final 