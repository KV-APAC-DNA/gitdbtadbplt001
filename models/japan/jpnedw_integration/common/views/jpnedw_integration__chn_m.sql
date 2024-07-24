with edi_chn_m as(
    select * from {{ ref('jpnedw_integration__edi_chn_m') }}
),
transformed as(
    SELECT edi_chn_m.create_dt
        ,edi_chn_m.create_user
        ,edi_chn_m.update_dt
        ,edi_chn_m.update_user
        ,edi_chn_m.reg_dt
        ,edi_chn_m.chn_cd
        ,edi_chn_m.lgl_nm
        ,edi_chn_m.cmmn_nm
        ,edi_chn_m.adrs
        ,edi_chn_m.acnt_prsn_cd
        ,edi_chn_m.rank
        ,edi_chn_m.chn_offc_cd
        ,edi_chn_m.frnc
        ,edi_chn_m.sgmt
        ,edi_chn_m.an_typ
        ,edi_chn_m.pj_typ
        ,edi_chn_m.sales_group
        ,edi_chn_m.scnd_acnt_prsn
    FROM edi_chn_m
)
select * from transformed