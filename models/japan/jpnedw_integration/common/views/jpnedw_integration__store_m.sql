with edi_store_m as(
    select * from {{ ref('jpnedw_integration__edi_store_m') }}
),
transformed as(
    SELECT edi_store_m.create_dt
        ,edi_store_m.create_user
        ,edi_store_m.update_dt
        ,edi_store_m.update_user
        ,edi_store_m.reg_dt
        ,edi_store_m.str_cd
        ,edi_store_m.lgl_nm_knj1
        ,edi_store_m.lgl_nm_knj2
        ,edi_store_m.lgl_nm_kn
        ,edi_store_m.cmmn_nm_knj
        ,edi_store_m.cmmn_nm_kn
        ,edi_store_m.adrs_knj1
        ,edi_store_m.adrs_knj2
        ,edi_store_m.adrs_kn
        ,edi_store_m.pst_co
        ,edi_store_m.tel_no
        ,edi_store_m.jis_prfct_c
        ,edi_store_m.jis_city_cd
        ,edi_store_m.trd_cd
        ,edi_store_m.trd_offc_cd
        ,edi_store_m.chn_cd
        ,edi_store_m.chn_offc_cd
        ,edi_store_m.chn_cd_oth
        ,edi_store_m.emp_cd_kk
        ,edi_store_m.all_str_ass
        ,edi_store_m.agrm_str
        ,edi_store_m.pj_ass
        ,edi_store_m.emp_cd_roc
    FROM edi_store_m
)
select * from transformed