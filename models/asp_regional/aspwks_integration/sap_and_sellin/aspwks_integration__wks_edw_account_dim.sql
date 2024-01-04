with itg_acct as (
    select * from {{ ref('aspitg_integration__itg_acct') }}
),

itg_acct_text as (
    select * from {{ ref('aspitg_integration__itg_acct_text') }}
),

--Join
edw_account_dim_temp as (
    select
        chrt_acct,
        acct_num,
        acct_nm,
        obj_ver,
        chg_flg,
        bal_flag,
        cstel_flag,
        glacc_flag,
        src_sys,
        sem_posit,
        bravo_acct_l1,
        bravo_acct_l2,
        bravo_acct_l3,
        bravo_acct_l4,
        bravo_acct_l5,
        fin_stat_itm
        -- tgt.crt_dttm as tgt_crt_dttm,
        -- updt_dttm,
        -- case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
    from (select a.*, b.med_desc as acct_nm
    from itg_acct as a
    left outer join itg_acct_text as b 
    on a.chrt_acct=b.chrt_acct and a.acct_num=b.acct_num and b.lang_key='E')
),

final as (
    select
        chrt_acct,
        acct_num,
        acct_nm,
        obj_ver,
        chg_flg,
        bal_flag,
        cstel_flag,
        glacc_flag,
        src_sys,
        sem_posit,
        bravo_acct_l1,
        bravo_acct_l2,
        bravo_acct_l3,
        bravo_acct_l4,
        bravo_acct_l5,
        fin_stat_itm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from edw_account_dim_temp
)

select * from final
