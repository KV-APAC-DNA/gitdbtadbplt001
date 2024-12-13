{{ config(
  materialized='view',
  secure='true'
) }}

select
    client::varchar(16777216) as client,
    cl_code::varchar(16777216) as cl_code,
    cl_gstin::varchar(16777216) as cl_gstin,
    product::varchar(16777216) as product,
    est_no::varchar(16777216) as est_no,
    est_dt::date as est_dt,
    est_amount::float as est_amount,
    est_rt10sc::float as est_rt10sc,
    supp_no::varchar(16777216) as supp_no,
    supp_dt::varchar(16777216) as supp_dt,
    cam_code::varchar(16777216) as cam_code,
    cam_des::varchar(16777216) as cam_des,
    cam_rem::varchar(16777216) as cam_rem,
    channel_cd::varchar(16777216) as channel_cd,
    channel::varchar(16777216) as channel,
    program::varchar(16777216) as program,
    days::varchar(16777216) as days,
    spot_date::date as spot_date,
    spot_day::varchar(16777216) as spot_day,
    ori_date::date as ori_date,
    tele_time::varchar(16777216) as tele_time,
    duration::integer as duration,
    rev_date::date as rev_date,
    status::varchar(16777216) as status,
    ro_number::varchar(16777216) as ro_number,
    ro_date::date as ro_date,
    ro_amount::float as ro_amount,
    ro_rt_10sc::float as ro_rt_10sc,
    can_no::varchar(16777216) as can_no,
    can_date::date as can_date,
    saveest_no::varchar(16777216) as saveest_no,
    currency::varchar(16777216) as currency,
    moni_comp::varchar(16777216) as moni_comp,
    moni_no::varchar(16777216) as moni_no,
    moni_date::date as moni_date,
    ad_spotord::integer as ad_spotord,
    tot_spot::integer as tot_spot,
    ad_time::varchar(16777216) as ad_time,
    ad_lang::varchar(16777216) as ad_lang,
    ob_number::varchar(16777216) as ob_number,
    ob_date::date as ob_date,
    ob_amount::float as ob_amount,
    ob_rt_10sc::float as ob_rt_10sc,
    vendor_cd::varchar(16777216) as vendor_cd,
    ib_number::varchar(16777216) as ib_number,
    ib_date::varchar(16777216) as ib_date,
    entrc::varchar(16777216) as entrc,
    ad_no::varchar(16777216) as ad_no,
    caption::varchar(16777216) as caption,
    rotocomp::varchar(16777216) as rotocomp,
    vn_gstin::varchar(16777216) as vn_gstin,
    pono::varchar(16777216) as pono,
    po_dt::date as po_dt,
    sapno::varchar(16777216) as sapno,
    fa_ib_no::varchar(16777216) as fa_ib_no,
    aac_amt::float as aac_amt,
    aac_per::float as aac_per,
    sac_per::float as sac_per,
    net_ro::float as net_ro,
    net_ob::float as net_ob,
    net_ib::float as net_ib,
    ro_client::varchar(16777216) as ro_client,
    cat1_code::varchar(16777216) as cat1_code,
    cat2_code::varchar(16777216) as cat2_code,
    cat1_name::varchar(16777216) as cat1_name,
    cat2_name::varchar(16777216) as cat2_name,
    std_rate::float as std_rate,
    std_cost::float as std_cost,
    plan_rate::float as plan_rate,
    plan_cost::float as plan_cost,
    bill_no,
    schmonth::integer as schmonth,
    schyear::integer as schyear,
    tape_no::varchar(16777216) as tape_no,
    adtypdesc::varchar(16777216) as adtypdesc,
    supbill_no::varchar(16777216) as supbill_no,
    supbill_dt::date as supbill_dt,
    supbillamt::float as supbillamt,
    supbillnet::float as supbillnet,
    ob_state::varchar(16777216) as ob_state,
    ob_gst_on::varchar(16777216) as ob_gst_on,
    agngstbase::float as agngstbase,
    agncgstper::float as agncgstper,
    agncomcgst::float as agncomcgst,
    agnsgstper::float as agnsgstper,
    agncomsgst::float as agncomsgst,
    agnigstper::float as agnigstper,
    agncomigst::float as agncomigst,
    recovertax::varchar(16777216) as recovertax,
    ob_gstbase::float as ob_gstbase,
    ob_cgstper::float as ob_cgstper,
    ob_cgstamt::float as ob_cgstamt,
    ob_sgstper::float as ob_sgstper,
    ob_sgstamt::float as ob_sgstamt,
    ob_igstper::float as ob_igstper,
    ob_igstamt::float as ob_igstamt,
    irn_no::varchar(16777216) as irn_no,
    ack_no::varchar(16777216) as ack_no,
    ack_dt::date as ack_dt,
    "group"::varchar(16777216) as "group",
    tel_stn::varchar(16777216) as tel_stn,
    key::varchar(16777216) as key,
    cl_total::varchar(16777216) as cl_total,
    rocl_cd::varchar(16777216) as rocl_cd,
    stventer::varchar(16777216) as stventer,
    est_spot::integer as est_spot,
    mediam::varchar(16777216) as mediam,
    pr_code::varchar(16777216) as pr_code,
    ob_cost_inr::float as ob_cost_inr,
    ob_cost_usd::float as ob_cost_usd,
    uidkey::varchar(16777216) as uidkey,
    comp_grpcd::varchar(16777216) as comp_grpcd,
    comp_grpnm::varchar(16777216) as comp_grpnm,
    house_name::varchar(16777216) as house_name,
    prov_on::varchar(16777216) as prov_on,
    gcgh_country::varchar(16777216) as gcgh_country,
    gcph_brand::varchar(16777216) as gcph_brand,
    brand_harmonized_by::varchar(16777216) as brand_harmonized_by,
    crt_dttm as crt_dttm
from {{ ref('inditg_integration__fct_traditional_tv_spends_india_monthly') }}
