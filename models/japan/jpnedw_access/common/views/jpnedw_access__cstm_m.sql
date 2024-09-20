with source as 
(
    select * from {{ ref('jpnedw_integration__cstm_m') }}
),
final as 
(
select 
    create_dt as "create_dt",
    create_user as "create_user",
    update_dt as "update_dt",
    update_user as "update_user",
    reg_dt as "reg_dt",
    cstm_cd as "cstm_cd",
    cstm_nm as "cstm_nm",
    cstm_nm_kn as "cstm_nm_kn",
    cstm_nm_knj as "cstm_nm_knj",
    adrs as "adrs",
    adrs_kn as "adrs_kn",
    adrs_knj as "adrs_knj",
    pst_cd as "pst_cd",
    tel_num as "tel_num",
    fax_nun as "fax_nun",
    plnt_cd as "plnt_cd",
    ship_dpt as "ship_dpt",
    ship_ld_tm as "ship_ld_tm",
    jis_prfct_cd as "jis_prfct_cd",
    jis_city_cd as "jis_city_cd",
    cstm_typ as "cstm_typ",
    cmnt1 as "cmnt1",
    bl_cls_dt as "bl_cls_dt",
    trd_typ_cg_flg as "trd_typ_cg_flg",
    jrsd_dpt_cd as "jrsd_dpt_cd",
    acnt_prsn_cd as "acnt_prsn_cd",
    buy_from_cd as "buy_from_cd",
    rebate_rep_cd as "rebate_rep_cd",
    updateflg as "updateflg",
    cust_tfi_num as "cust_tfi_num",
    transporter_id as "transporter_id",
    transport_fee_id as "transport_fee_id",
    transport_timing as "transport_timing"
from source
)
select * from final