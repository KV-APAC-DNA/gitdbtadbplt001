with edw_account_dim as (
    select * from {{ ref('aspedw_integration__edw_account_dim') }}
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
        fin_stat_itm as "fin_stat_itm",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"

    from edw_account_dim
)

select * from final
