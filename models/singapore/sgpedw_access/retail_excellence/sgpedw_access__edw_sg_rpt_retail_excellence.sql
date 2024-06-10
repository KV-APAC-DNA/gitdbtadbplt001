with edw_sg_rpt_retail_excellence_details as (
    select * from {{ ref('sgpedw_integration__edw_sg_rpt_retail_excellence') }}
),

final as (
    select
        chrt_acct as "chrt_acct",
        acct_num as "acct_num",
        acct_nm as "acct_nm",
        obj_ver as "obj_ver",
        chg_flg as "chg_flg",
        bal_flag as "bal_flag",
        cstel_flag as "cstel_flag",
        glacc_flag as "glacc_flag",
        src_sys as "src_sys",
        sem_posit as "sem_posit",
        bravo_acct_l1 as "bravo_acct_l1",
        bravo_acct_l2 as "bravo_acct_l2",
        bravo_acct_l3 as "bravo_acct_l3",
        bravo_acct_l4 as "bravo_acct_l4",
        bravo_acct_l5 as "bravo_acct_l5",
        bravo_acct_l6 as "bravo_acct_l6",
        ciw_acct_l1 as "ciw_acct_l1",
        ciw_acct_l2 as "ciw_acct_l2",
        ciw_acct_l3 as "ciw_acct_l3",
        ciw_acct_l4 as "ciw_acct_l4",
        ciw_acct_l5 as "ciw_acct_l5",
        ciw_acct_l6 as "ciw_acct_l6",
        bravo_acct_l1_txt as "bravo_acct_l1_txt",
        bravo_acct_l2_txt as "bravo_acct_l2_txt",
        bravo_acct_l3_txt as "bravo_acct_l3_txt",
        bravo_acct_l4_txt as "bravo_acct_l4_txt",
        bravo_acct_l5_txt as "bravo_acct_l5_txt",
        bravo_acct_l6_txt as "bravo_acct_l6_txt",
        ciw_acct_l1_txt as "ciw_acct_l1_txt",
        ciw_acct_l2_txt as "ciw_acct_l2_txt",
        ciw_acct_l3_txt as "ciw_acct_l3_txt",
        ciw_acct_l4_txt as "ciw_acct_l4_txt",
        ciw_acct_l5_txt as "ciw_acct_l5_txt",
        ciw_acct_l6_txt as "ciw_acct_l6_txt",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"

    from edw_account_ciw_dim
)

select * from final
