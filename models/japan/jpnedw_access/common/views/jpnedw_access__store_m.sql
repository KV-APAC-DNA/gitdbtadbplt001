with source as(
    select * from {{ ref('jpnedw_integration__store_m') }}
),
transformed as(
    SELECT create_dt as "create_dt"
        ,create_user as "create_user"
        ,update_dt as "update_dt"
        ,update_user as "update_user"
        ,reg_dt as "reg_dt"
        ,str_cd as "str_cd"
        ,lgl_nm_knj1 as "lgl_nm_knj1"
        ,lgl_nm_knj2 as "lgl_nm_knj2"
        ,lgl_nm_kn as "lgl_nm_kn"
        ,cmmn_nm_knj as "cmmn_nm_knj"
        ,cmmn_nm_kn as "cmmn_nm_kn"
        ,adrs_knj1 as "adrs_knj1"
        ,adrs_knj2 as "adrs_knj2"
        ,adrs_kn as "adrs_kn"
        ,pst_co as "pst_co"
        ,tel_no as "tel_no"
        ,jis_prfct_c as "jis_prfct_c"
        ,jis_city_cd as "jis_city_cd"
        ,trd_cd as "trd_cd"
        ,trd_offc_cd as "trd_offc_cd"
        ,chn_cd as "chn_cd"
        ,chn_offc_cd as "chn_offc_cd"
        ,chn_cd_oth as "chn_cd_oth"
        ,emp_cd_kk as "emp_cd_kk"
        ,all_str_ass as "all_str_ass"
        ,agrm_str as "agrm_str"
        ,pj_ass as "pj_ass"
        ,emp_cd_roc as "emp_cd_roc"
    FROM source
)
select * from transformed