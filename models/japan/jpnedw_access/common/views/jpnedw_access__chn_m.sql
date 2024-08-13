with source as 
(
    select * from {{ ref('jpnedw_integration__chn_m') }}
),
final as 
(
select 
    create_dt as "create_dt",
    create_user as "create_user",
    update_dt as "update_dt",
    update_user as "update_user",
    reg_dt as "reg_dt",
    chn_cd as "chn_cd",
    lgl_nm as "lgl_nm",
    cmmn_nm as "cmmn_nm",
    adrs as "adrs",
    acnt_prsn_cd as "acnt_prsn_cd",
    rank as "rank",
    chn_offc_cd as "chn_offc_cd",
    frnc as "frnc",
    sgmt as "sgmt",
    an_typ as "an_typ",
    pj_typ as "pj_typ",
    sales_group as "sales_group",
    scnd_acnt_prsn as "scnd_acnt_prsn"
from source
)
select * from final