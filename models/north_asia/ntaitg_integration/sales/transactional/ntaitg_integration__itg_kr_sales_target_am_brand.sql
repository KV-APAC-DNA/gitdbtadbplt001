{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} itg  using {{ ref('ntawks_integration__wks_itg_kr_sales_target_am_brand') }} wks
                    where wks.year = itg.yr and wks.country_code = itg.ctry_cd and wks.chng_flg = 'U';
                    {% endif %}
                    "
    )
}}

with source as (
    select * from {{ ref('ntawks_integration__wks_itg_kr_sales_target_am_brand') }}
),
transformed as 
(
    select 
        brand,
        account_manager,
        year,
        jan_trgt_amt,
        feb_trgt_amt,
        mar_trgt_amt,
        apr_trgt_amt,
        may_trgt_amt,
        jun_trgt_amt,
        jul_trgt_amt,
        aug_trgt_amt,
        sep_trgt_amt,
        oct_trgt_amt,
        nov_trgt_amt,
        dec_trgt_amt,
        country_code,
        CASE
            WHEN CHNG_FLG = 'I' THEN convert_timezone('UTC', current_timestamp())
            ELSE TGT_CRT_DTTM
        END AS crt_dttm,
        convert_timezone('UTC', current_timestamp()) AS updt_dttm
    FROM source   
),
final as 
(
    select 
        brand::varchar(30) as brnd,
        account_manager::varchar(30) as acct_mgr,
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
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final
