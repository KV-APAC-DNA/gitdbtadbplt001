with source as (
    select * from {{ ref ('pcfedw_integration__vw_competitive_banner_group') }}
),
final as (
    select 
        market AS "market",
        banner AS "banner",
        banner_classification AS "banner_classification",
        manufacturer AS "manufacturer",
        brand AS "brand",
        sku_name AS "sku_name",
        ean_number AS "ean_number",
        unit AS "unit",
        dollar AS "dollar",
        year AS "year",
        month AS "month",
        quarter AS "quarter",
        jj_mnth AS "jj_mnth",
        jj_qrtr AS "jj_qrtr",
        jj_year AS "jj_year",
        country AS "country",
        currency AS "currency",
        crt_dttm AS "crt_dttm"
    from source
)
select * from final