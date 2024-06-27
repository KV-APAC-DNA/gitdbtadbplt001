{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["prft_ctr","crncy_cd","cust_channel","ctry_cd","yr","cust_hdqtr_cd","cust_hdqtr_nm"],
        merge_exclude_columns=["crt_dttm"]
)}}
with source as (
    select * from {{ ref('ntawks_integration__wks_itg_trd_promo_pln') }}
),
final as (
    select
        profit_center::varchar(100) as prft_ctr,
        profit_center_nm::varchar(100) as prft_ctr_nm,
        customer_channel::varchar(100) as cust_channel,
        customer_channel_nm::varchar(100) as cust_channel_nm,
        customer_hq_code::varchar(200) as cust_hdqtr_cd,
        customer_hq_nm::varchar(200) as cust_hdqtr_nm,
        country_code::varchar(20) as ctry_cd,
        currency_code::varchar(3) as crncy_cd,
        year::varchar(4) as yr,
        jan_tp_plan::number(16,2) as jan_trd_promo_pln,
        feb_tp_plan::number(16,2) as feb_trd_promo_pln,
        mar_tp_plan::number(16,2) as mar_trd_promo_pln,
        apr_tp_plan::number(16,2) as apr_trd_promo_pln,
        may_tp_plan::number(16,2) as may_trd_promo_pln,
        jun_tp_plan::number(16,2) as jun_trd_promo_pln,
        jul_tp_plan::number(16,2) as jul_trd_promo_pln,
        aug_tp_plan::number(16,2) as aug_trd_promo_pln,
        sep_tp_plan::number(16,2) as sep_trd_promo_pln,
        oct_tp_plan::number(16,2) as oct_trd_promo_pln,
        nov_tp_plan::number(16,2) as nov_trd_promo_pln,
        dec_tp_plan::number(16,2) as dec_trd_promo_pln,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final