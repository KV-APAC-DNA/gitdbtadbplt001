with edi_cstm_m as(
    select * from {{ ref('jpnedw_integration__edi_cstm_m') }}
),
transformed as(
    SELECT edi_cstm_m.create_dt
        ,edi_cstm_m.create_user
        ,edi_cstm_m.update_dt
        ,edi_cstm_m.update_user
        ,edi_cstm_m.reg_dt
        ,edi_cstm_m.cstm_cd
        ,edi_cstm_m.cstm_nm
        ,edi_cstm_m.cstm_nm_kn
        ,edi_cstm_m.cstm_nm_knj
        ,edi_cstm_m.adrs
        ,edi_cstm_m.adrs_kn
        ,edi_cstm_m.adrs_knj
        ,edi_cstm_m.pst_cd
        ,edi_cstm_m.tel_num
        ,edi_cstm_m.fax_nun
        ,edi_cstm_m.plnt_cd
        ,edi_cstm_m.ship_dpt
        ,edi_cstm_m.ship_ld_tm
        ,edi_cstm_m.jis_prfct_cd
        ,edi_cstm_m.jis_city_cd
        ,edi_cstm_m.cstm_typ
        ,edi_cstm_m.cmnt1
        ,edi_cstm_m.bl_cls_dt
        ,edi_cstm_m.trd_typ_cg_flg
        ,edi_cstm_m.jrsd_dpt_cd
        ,edi_cstm_m.acnt_prsn_cd
        ,edi_cstm_m.buy_from_cd
        ,edi_cstm_m.rebate_rep_cd
        ,edi_cstm_m.updateflg
        ,edi_cstm_m.cust_tfi_num
        ,edi_cstm_m.transporter_id
        ,edi_cstm_m.transport_fee_id
        ,edi_cstm_m.transport_timing
    FROM edi_cstm_m
)
select * from transformed