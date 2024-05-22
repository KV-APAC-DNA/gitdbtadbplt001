{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_grp","acct_mgr","brnd","ctry_cd","yr"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}
with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_sales_target_am_sls_grp') }}
),
final as 
(
    select 
        sales_group::varchar(30) as sls_grp,
        brnd::varchar(30) as brnd,
        acct_mgr::varchar(2000) as acct_mgr,
        year::number(18,0) as yr,
        jan_trgt_amt::number(16,5) as jan_trgt_amt,
        feb_trgt_amt::number(16,5) as feb_trgt_amt,
        mar_trgt_amt::number(16,5) as mar_trgt_amt,
        apr_trgt_amt::number(16,5) as apr_trgt_amt,
        may_trgt_amt::number(16,5) as may_trgt_amt,
        jun_trgt_amt::number(16,5) as jun_trgt_amt,
        jul_trgt_amt::number(16,5) as jul_trgt_amt,
        aug_trgt_amt::number(16,5) as aug_trgt_amt,
        sep_trgt_amt::number(16,5) as sep_trgt_amt,
        oct_trgt_amt::number(16,5) as oct_trgt_amt,
        nov_trgt_amt::number(16,5) as nov_trgt_amt,
        dec_trgt_amt::number(16,5) as dec_trgt_amt,
        country_code::varchar(10) as ctry_cd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final