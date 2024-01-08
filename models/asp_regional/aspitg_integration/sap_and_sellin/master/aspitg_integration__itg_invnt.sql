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
    request_number as request_number,
    data_packet as data_packet,
    data_record as data_record,
    loc_currcy as lcl_crncy,
    base_uom as base_unit,
    material as matl_no,
    stor_loc as strg_loc,
    stocktype as stk_type,
    stockcat as stk_cat,
    comp_code as co_cd,
    mat_plant as matl_plnt_view,
    bic_zbatch as btch_num,
    plant as plnt,
    whse_num as whse_num,
    strge_bin as strg_bin,
    strge_type as strg_type,
    indspecstk as spl_stck_val,
    bic_zmdsobkz as spl_stck_indica,
    doc_date as doc_dt,
    val_class as valut_cls,
    id_valarea as valut_area,
    val_type as valut_type,
    pstng_date as pstng_dt,
    calday as cal_day,
    bic_zmat_wh as wh_mstr,
    version as vers,
    vtype as val_type,
    vendor as vend,
    sold_to as sold_to_prty,
    bic_zmoveinds as mvmt_ind_sec,
    currency as crncy,
    fiscvarnt as fisc_yr_vrnt,
    fiscyear as fisc_yr,
    calyear as cal_yr,
    calmonth2 as cal_mo,
    calquart1 as qtr,
    calquarter as cal_yr_qtr,
    calweek as cal_yr_wk,
    fiscper3 as pstng_per,
    halfyear1 as half_yr,
    weekday1 as wkday,
    fiscper as fisc_yr_per,
    calmonth as cal_yr_mon,
    recvs_val as stck_rec_val,
    issvs_val as iss_stck_val,
    issblostck as iss_blok_qty,
    isscnsstck as cng_stk_qty_iss,
    issqmstck as iss_qty_qual,
    isstransst as iss_qty_trst,
    recblostck as rcpt_qty_blok,
    reccnsstck as cns_stck_rcpt,
    recqmstck as rcpt_qty_qual,
    rectransst as rcpt_qty_trst,
    issscrp as iss_qty_scrap,
    issvalscrp as iss_val_scrap,
    rectotstck as rcpt_tot_stck,
    isstotstck as iss_tot_stck,
    issvalstck as iss_qty_stck_val,
    recvalstck as rec_qty_val_stck,
    venconcon as vdr_cnsgnmnt_stck_cnval,
    recvalqm as rec_val_q_stck,
    recvalblo as rec_val_blok,
    issvalbloc as iss_val_blok,
    issvalqm as iss_val_q_stck,
    rectrfstva as rec_val_trst,
    isstrfstva as iss_val_trst,
    issccnsval as iss_val_cons_stck,
    recccnsval as rcpt_val_cons_stk,
    cppvlc as bw_prch_val,
    cpquabu as bw_amt_bunitm,
    bic_zmddelct as delv_cost,
    g_avv040 as std_matl_cost,
    price_unit as prc_unit,
    bic_zlincount as line_cnt,
    current_timestamp()::timestamp_ntz(9) as crt_dttm, 
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final