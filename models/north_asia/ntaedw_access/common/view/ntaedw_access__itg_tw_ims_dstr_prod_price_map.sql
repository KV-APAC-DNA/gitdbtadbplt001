with source as (
    select * from {{ ref('ntaitg_integration__itg_tw_ims_dstr_prod_price_map') }}
),
final as (
    select
        dstr_cd as "dstr_cd",
        dstr_nm as "dstr_nm",
        ean_cd as "ean_cd",
        dstr_prod_cd as "dstr_prod_cd",
        dstr_prod_nm as "dstr_prod_nm",
        sell_out_price_manual as "sell_out_price_manual",
        promotion_start_date as "promotion_start_date",
        promotion_end_date as "promotion_end_date",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final