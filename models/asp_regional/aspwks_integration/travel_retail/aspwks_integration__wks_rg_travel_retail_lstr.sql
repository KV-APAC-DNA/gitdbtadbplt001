with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_lstr') }}
),
final as (
    select
        trim(year) as year,
        trim(month) as month,
        trim(yearmo) as yearmo,
        trim(retailer_name) as retailer_name,
        trim(brand_name) as brand_name,
        trim(ean) as ean,
        trim(dcl_code) as dcl_code,
        trim(english_desc) as english_desc,
        trim(chinese_desc) as chinese_desc,
        trim(category) as category,
        trim(srp_usd) as srp_usd,
        trim(sls_qty_total) as sls_qty_total,
        trim(sls_amt_total) as sls_amt_total,
        trim(offline_qty) as offline_qty,
        trim(offline_amt) as offline_amt,
        trim(online_qty) as online_qty,
        trim(online_amt) as online_amt,
        trim(stock) as stock,
        trim(filename) as filename,
        trim(crttd) as crttd
    from source
)
select * from final