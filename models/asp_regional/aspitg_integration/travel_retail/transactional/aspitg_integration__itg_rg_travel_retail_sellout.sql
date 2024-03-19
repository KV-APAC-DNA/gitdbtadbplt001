{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key=["hash_key"]
    )
}}

--Import CTE
with dfs_hainan as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_dfs_hainan') }}
),
dufry_hainan as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_dufry_hainan') }}
),
lstr as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_lstr') }}
),
sales_stock as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_sales_stock') }}
),
shilla as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_shilla') }}
),
cdfg as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_cdfg') }}
),
cnsc as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_cnsc') }}
),
dfs as (
    select * from {{ ref('aspwks_integration__wks_rg_travel_retail_dfs') }}
),

--Logical CTE
trans_dfs_hainan as (
    select 'CN' as ctry_cd,
    'RMB' as crncy_cd,
    upper(retailer_name) as retailer_name,
    cast(substring(yearmo, 1, 4) as int) as year,
    cast(substring(yearmo, 6, 2) as int) as month,
    concat(
        coalesce(cast(substring(yearmo, 1, 4) as text), ''),
        coalesce(cast(substring(yearmo, 6, 2) as text), '')
    ) as year_month,
    brand as brand,
    reatiler_product_code as sku,
    reatiler_product_description as product_description,
    dcl_code,
    ean,
    null as rsp,
    upper(door_name) as door_name,
    coalesce(sls_mtd_qty, 0) as sls_qty,
    coalesce(sls_mtd_amt, 0) as sls_amt,
    null as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    case
        when door_name = 'HNN MH OFFLINE STORE 0904' then sls_mtd_qty
        else 0
    end as store_sls_qty,
    case
        when door_name = 'HNN MH OFFLINE STORE 0904' then sls_mtd_amt
        else 0
    end as store_sls_amt,
    case
        when door_name = 'HAINAN E-SHOP 0903' then sls_mtd_qty
        else 0
    end as ecommerce_sls_qty,
    case
        when door_name = 'HAINAN E-SHOP 0903' then sls_mtd_amt
        else 0
    end as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from dfs_hainan
),
trans_dufry_hainan as (
    select 'CN' as ctry_cd,
    'RMB' as crncy_cd,
    upper(retailer_name) as retailer_name,
    year as year,
    month as month,
    yearmo as year_month,
    brand as brand,
    null as sku,
    product_desc as product_description,
    dcl_code,
    ean,
    null as rsp,
    'DUFRY HAINAN' as door_name,
    coalesce(total_qty, 0) as sls_qty,
    coalesce(total_gmv, 0) as sls_amt,
    coalesce(stock, 0) as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    coalesce(offline_qty, 0) as store_sls_qty,
    coalesce(offline_gmv, 0) as store_sls_amt,
    coalesce(online_qty, 0) as ecommerce_sls_qty,
    coalesce(online_gmv, 0) as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from dufry_hainan
),
trans_lstr as (
    select 'CN' as ctry_cd,
    'RMB' as crncy_cd,
    upper(retailer_name) as retailer_name,
    cast(year as int) as year,
    cast(month as int) as month,
    yearmo as year_month,
    brand_name as brand,
    null as sku,
    english_desc as product_description,
    dcl_code as dcl_code,
    ean as ean,
    null as rsp,
    'LAGADERE HAINAN' as door_name,
    coalesce(sls_qty_total, 0) as sls_qty,
    coalesce(sls_amt_total, 0) as sls_amt,
    coalesce(stock, 0) as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    coalesce(offline_qty, 0) as store_sls_qty,
    coalesce(offline_amt, 0) as store_sls_amt,
    coalesce(online_qty, 0) as ecommerce_sls_qty,
    coalesce(online_amt, 0) as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from lstr
),
trans_sales_stock as (
    select 'KR' as ctry_cd,
    'KRW' as crncy_cd,
    case
        when upper(retailer_name) in ('LTJ', 'LTM', 'LTW') then 'LOTTE'
        when upper(retailer_name) in ('SLJ', 'SLM') then 'SHILLA'
        when upper(retailer_name) in ('SGB', 'SGM') then 'SSG'
        when upper(retailer_name) in ('HDC') then 'HDC'
        when upper(retailer_name) in ('DOOTA') then 'DOOTA'
        when upper(retailer_name) in ('HANWHA') then 'HANWHA'
        when upper(retailer_name) in ('DONGWHA') then 'DONGWHA'
        when upper(retailer_name) in ('HYUNDAI DDM', 'HYUNDAI COEX') then 'HYUNDAI'
    end as retailer_name,
    cast(year as int) as year,
    cast(month as int) as month,
    (year || month) as year_month,
    null as brand,
    sap_code as sku,
    product_desc as product_description,
    dcl_code,
    null as ean,
    iff(rsp='',null,rsp) as rsp,
    upper(location_name) as door_name,
    coalesce(iff(sls_qty='',null,sls_qty), 0) as sls_qty,
    coalesce(
        (
            cast(sls_qty as decimal(21, 5)) * cast(rsp as decimal(21, 5))
        ),
        0
    ) as sls_amt,
    coalesce(iff(stock_qty='',null,stock_qty), 0) as stock_qty,
    coalesce(
        (
            cast(stock_qty as decimal(21, 5)) * cast(rsp as decimal(21, 5))
        ),
        0
    ) as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    null as store_sls_qty,
    null as store_sls_amt,
    null as ecommerce_sls_qty,
    null as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(door_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from sales_stock
),
trans_shilla as (
    select 'SG' as ctry_cd,
    'SGD' as crncy_cd,
    upper(retailer_name) as retailer_name,
    cast(substring(year_month, 1, 4) as int) as year,
    cast(substring(year_month, 5, 6) as int) as month,
    year_month,
    brand,
    sku,
    description as product_description,
    dcl_code,
    ean,
    null as rsp,
    upper(location_name) as door_name,
    coalesce(iff(sls_qty='',null,sls_qty), 0) as sls_qty,
    coalesce(cast(iff(sls_amt='',null,sls_amt) as decimal(21, 5)), 0) as sls_amt,
    null as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    null as store_sls_qty,
    null as store_sls_amt,
    null as ecommerce_sls_qty,
    null as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from shilla
),
trans_cdfg as (
    select
    case
        when upper(location_name) in ('HTB', 'BEIJING AIRPORT', 'HAIKOU BUGOU', 'MEMBERS')
        then 'CN'
        when upper(location_name) = 'CAMBODIA DOWNTOWN'
        then 'CM'
        else null
    end as ctry_cd,
    case
        when upper(location_name) in ('HTB', 'BEIJING AIRPORT', 'HAIKOU BUGOU', 'MEMBERS')
        then 'RMB'
        when upper(location_name) = 'CAMBODIA DOWNTOWN'
        then 'KHR'
        else null
    end as crncy_cd,
    upper(retailer_name) as retailer_name,
    cast(substring(year_month, 1, 4) as int) as year,
    cast(substring(year_month, 5, 6) as int) as month,
    year_month,
    null as brand,
    null as sku,
    description as product_description,
    dcl_code,
    barcode as ean,
    null as rsp,
    upper(location_name) as door_name,
    coalesce(iff(sls_qty='',null,sls_qty), 0) as sls_qty,
    null as sls_amt,
    coalesce(iff(stock_qty='',null,stock_qty), 0) as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    null as store_sls_qty,
    null as store_sls_amt,
    null as ecommerce_sls_qty,
    null as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from cdfg
),
trans_cnsc as (
    select
    'CN' as ctry_cd,
    'RMB' as crncy_cd,
    upper(retailer_name) as retailer_name,
    cast(substring(yearmo, 1, 4) as int) as year,
    cast(substring(yearmo, 5, 6) as int) as month,
    yearmo as year_month,
    brand,
    material_code as sku,
    product_description,
    dcl_code,
    ean,
    null as rsp,
    upper(door_name) as door_name,
    coalesce(sales_qty, 0) as sls_qty,
    coalesce(sales_amount, 0) as sls_amt,
    coalesce(inventory_qty, 0) as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    coalesce(store_sales, 0) as store_sls_qty,
    coalesce(total_store_sales, 0) as store_sls_amt,
    coalesce(no_of_ecommerce_sales, 0) as ecommerce_sls_qty,
    coalesce(total_ecommerce_sales, 0) as ecommerce_sls_amt,
    coalesce(membership_sls_qty, 0) as membership_sls_qty,
    coalesce(membership_sls_amt, 0) as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from cnsc
),
trans_dfs as (
    select
    'HK' as ctry_cd,
    'HKD' as crncy_cd,
    upper(retailer_name) as retailer_name,
    cast(substring(year_month, 1, 4) as int) as year,
    cast(substring(year_month, 5, 6) as int) as month,
    year_month,
    brand,
    common_sku as sku,
    style as product_description,
    case
        when vendor_style like '%D40%'
        then split_part(vendor_style, '_', 2)
        else vendor_style
    end as dcl_code,
    null as ean,
    null as rsp,
    upper(location_name) as door_name,
    coalesce(iff(sls_qty='',null,sls_qty), 0) as sls_qty,
    coalesce(cast(iff(sls_amt='',null,sls_amt) as decimal(21, 5)), 0) as sls_amt,
    coalesce(iff(soh_qty='',null,soh_qty), 0) as stock_qty,
    null as stock_amt,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm,
    null as store_sls_qty,
    null as store_sls_amt,
    null as ecommerce_sls_qty,
    null as ecommerce_sls_amt,
    null as membership_sls_qty,
    null as membership_sls_amt,
    md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) as hash_key
    from dfs
),
transformed as (
    select * from trans_dfs_hainan
    union all
    select * from trans_dufry_hainan
    union all
    select * from trans_lstr
    union all
    select * from trans_sales_stock
    union all
    select * from trans_shilla
    union all
    select * from trans_cdfg
    union all
    select * from trans_cnsc
    union all
    select * from trans_dfs
),
--Final CTE
final as (
    select 
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        retailer_name::varchar(50) as retailer_name,
        year::number(18,0) as year,
        month::number(18,0) as month,
        year_month::varchar(10) as year_month,
        brand::varchar(50) as brand,
        sku::varchar(50) as sku,
        product_description::varchar(100) as product_description,
        dcl_code::varchar(50) as dcl_code,
        ean::varchar(50) as ean,
        rsp::number(18,0) as rsp,
        door_name::varchar(50) as door_name,
        sls_qty::number(18,0) as sls_qty,
        sls_amt::number(21,5) as sls_amt,
        stock_qty::number(18,0) as stock_qty,
        stock_amt::number(21,5) as stock_amt,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        store_sls_qty::number(18,0) as store_sls_qty,
        store_sls_amt::number(38,18) as store_sls_amt,
        ecommerce_sls_qty::number(18,0) as ecommerce_sls_qty,
        ecommerce_sls_amt::number(38,18) as ecommerce_sls_amt,
        membership_sls_qty::number(18,0) as membership_sls_qty,
        membership_sls_amt::number(38,18) as membership_sls_amt,
        hash_key
    from transformed
)

--Final select
select * from final
