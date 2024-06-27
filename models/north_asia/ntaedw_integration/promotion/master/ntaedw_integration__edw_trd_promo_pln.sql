{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["prft_ctr","prft_ctr_nm","crncy_cd","cust_channel","cust_channel_nm","ctry_cd","yr","cust_hdqtr_cd","cust_hdqtr_nm"],
        merge_exclude_columns=["crt_dttm"]
)}}
with source as (
    select * from {{ ref('ntaitg_integration__itg_trd_promo_pln') }}
),
final as (
    select
        prft_ctr::varchar(100) as prft_ctr,
        prft_ctr_nm::varchar(100) as prft_ctr_nm,
        cust_channel::varchar(100) as cust_channel,
        cust_channel_nm::varchar(100) as cust_channel_nm,
        cust_hdqtr_cd::varchar(200) as cust_hdqtr_cd,
        cust_hdqtr_nm::varchar(200) as cust_hdqtr_nm,
        ctry_cd::varchar(20) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        yr::varchar(4) as yr,
        jan_trd_promo_pln::number(16,2) as jan_trd_promo_pln,
        feb_trd_promo_pln::number(16,2) as feb_trd_promo_pln,
        mar_trd_promo_pln::number(16,2) as mar_trd_promo_pln,
        apr_trd_promo_pln::number(16,2) as apr_trd_promo_pln,
        may_trd_promo_pln::number(16,2) as may_trd_promo_pln,
        jun_trd_promo_pln::number(16,2) as jun_trd_promo_pln,
        jul_trd_promo_pln::number(16,2) as jul_trd_promo_pln,
        aug_trd_promo_pln::number(16,2) as aug_trd_promo_pln,
        sep_trd_promo_pln::number(16,2) as sep_trd_promo_pln,
        oct_trd_promo_pln::number(16,2) as oct_trd_promo_pln,
        nov_trd_promo_pln::number(16,2) as nov_trd_promo_pln,
        dec_trd_promo_pln::number(16,2) as dec_trd_promo_pln,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final