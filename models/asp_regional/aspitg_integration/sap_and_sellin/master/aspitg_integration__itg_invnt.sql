{{
    config(
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ["request_number","data_packet","data_record"],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE

with source as (
    select * from {{ ref('aspwks_integration__wks_itg_invnt')}}
),

--Logical CTE
final as(
    select
    request_number::VARCHAR(100) as request_number,
    data_packet::VARCHAR(50) as data_packet,
    data_record::VARCHAR(100) as data_record,
    loc_currcy::VARCHAR(5) as lcl_crncy,
    base_uom::VARCHAR(3) as base_unit,
    material::VARCHAR(18) as matl_no,
    stor_loc::VARCHAR(4) as strg_loc,
    stocktype::VARCHAR(1) as stk_type,
    stockcat::VARCHAR(1) as stk_cat,
    comp_code::VARCHAR(4) as co_cd,
    mat_plant::VARCHAR(18) as matl_plnt_view,
    bic_zbatch::VARCHAR(10) as btch_num,
    plant::VARCHAR(4) as plnt,
    whse_num::VARCHAR(3) as whse_num,
    strge_bin::VARCHAR(10) as strg_bin,
    strge_type::VARCHAR(3) as strg_type,
    indspecstk::VARCHAR(1) as spl_stck_val,
    bic_zmdsobkz::VARCHAR(1) as spl_stck_indica,
    doc_date::DATE as doc_dt,
    val_class::VARCHAR(4) as valut_cls,
    id_valarea::VARCHAR(4) as valut_area,
    val_type::VARCHAR(10) as valut_type,
    pstng_date::DATE as pstng_dt,
    calday::DATE as cal_day,
    bic_zmat_wh::VARCHAR(18) as wh_mstr,
    version::VARCHAR(3) as vers,
    vtype::NUMBER(18,0) as val_type,
    vendor::VARCHAR(10) as vend,
    sold_to::VARCHAR(10) as sold_to_prty,
    bic_zmoveinds::VARCHAR(1) as mvmt_ind_sec,
    currency::VARCHAR(5) as crncy,
    fiscvarnt::VARCHAR(2) as fisc_yr_vrnt,
    fiscyear::NUMBER(18,0) as fisc_yr,
    calyear::NUMBER(18,0) as cal_yr,
    calmonth2::NUMBER(18,0) as cal_mo,
    calquart1::NUMBER(18,0) as qtr,
    calquarter::NUMBER(18,0) as cal_yr_qtr,
    calweek::NUMBER(18,0) as cal_yr_wk,
    fiscper3::NUMBER(18,0) as pstng_per,
    halfyear1::NUMBER(18,0) as half_yr,
    weekday1::NUMBER(18,0) as wkday,
    fiscper::NUMBER(18,0) as fisc_yr_per,
    calmonth::NUMBER(18,0) as cal_yr_mon,
    recvs_val::NUMBER(19,2) as stck_rec_val,
    issvs_val::NUMBER(19,2) as iss_stck_val,
    issblostck::NUMBER(20,3) as iss_blok_qty,
    isscnsstck::NUMBER(20,3) as cng_stk_qty_iss,
    issqmstck::NUMBER(20,3)as iss_qty_qual,
    isstransst::NUMBER(20,3) as iss_qty_trst,
    recblostck::NUMBER(20,3) as rcpt_qty_blok,
    reccnsstck::NUMBER(20,3) as cns_stck_rcpt,
    recqmstck::NUMBER(20,3) as rcpt_qty_qual,
    rectransst::NUMBER(20,3) as rcpt_qty_trst,
    issscrp::NUMBER(20,3) as iss_qty_scrap,
    issvalscrp::NUMBER(19,2) as iss_val_scrap,
    rectotstck::NUMBER(20,3) as rcpt_tot_stck,
    isstotstck::NUMBER(20,3) as iss_tot_stck,
    issvalstck::NUMBER(20,3) as iss_qty_stck_val,
    recvalstck::NUMBER(20,3) as rec_qty_val_stck,
    venconcon::NUMBER(19,2) as vdr_cnsgnmnt_stck_cnval,
    recvalqm::NUMBER(19,2) as rec_val_q_stck,
    recvalblo::NUMBER(19,2) as rec_val_blok,
    issvalbloc::NUMBER(19,2) as iss_val_blok,
    issvalqm::NUMBER(19,2) as iss_val_q_stck,
    rectrfstva::NUMBER(19,2) as rec_val_trst,
    isstrfstva::NUMBER(19,2) as iss_val_trst,
    issccnsval::NUMBER(19,2) as iss_val_cons_stck,
    recccnsval::NUMBER(19,2) as rcpt_val_cons_stk,
    cppvlc::NUMBER(19,2) as bw_prch_val,
    cpquabu::NUMBER(20,3) as bw_amt_bunitm,
    bic_zmddelct::NUMBER(19,2) as delv_cost,
    g_avv040::NUMBER(19,2) as std_matl_cost,
    price_unit::NUMBER(20,3) as prc_unit,
    bic_zlincount::NUMBER(18,0) as line_cnt,
    current_timestamp()::timestamp_ntz(9) as crt_dttm, 
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final