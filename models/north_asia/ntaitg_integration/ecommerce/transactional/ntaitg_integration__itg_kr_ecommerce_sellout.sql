{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date in (select distinct upper(cust_nm) || ean_num || to_date(transaction_date, 'MM/DD/YYYY') from {{ ref('ntawks_integration__wks_kr_ecommerce_naver_sellout') }});
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date in (select distinct upper(cust_nm) || ean_num || coalesce(try_to_date(transaction_date, 'YYYYMMDD'), to_date(transaction_date, 'DD/MM/YY')) from {{ ref('ntawks_integration__wks_kr_ecom_coupang') }});
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date in (select distinct upper(cust_nm) || ean_num || to_date(transaction_date, 'YYYYMMDD') from {{ ref('ntawks_integration__wks_kr_ecommerce_ebay_sellout') }});
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date in (select distinct upper(cust_nm) || ean_num || to_date(transaction_date, 'YYYYMMDD') from {{ ref('ntawks_integration__wks_kr_ecommerce_trexi_sellout') }});
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date in (select distinct upper(customer_name) || ean_number || to_date(transaction_date) from {{ ref('ntawks_integration__wks_kr_ecommerce_unitoa_sellout') }});
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date in (select distinct upper(customer_name) || ean_number || to_date(transaction_date) from {{ ref('ntawks_integration__wks_kr_ecommerce_tca_sellout') }});
        delete from {{this}} where upper(customer_name) || ean_number || transaction_date || upper(sub_customer_name) in (select distinct 'EMART' || nvl(ean, offline_ean) || to_date(pos_dt, 'YYYYMMDD') || 'SSG.COM' from {{ ref('ntawks_integration__wks_kr_pos_emart_ssg') }});
        delete from {{this}} where upper(customer_name) || nvl(ean_number, '#') || transaction_date || upper(sub_customer_name) in (select distinct 'EMART' || nvl(ean, '#') || cast(pos_dt as date) || 'ECVAN' from {{ ref('ntawks_integration__wks_sdl_kr_pos_emart_ecvan') }});
        {% endif %}"
    )
}}

with itg_sales_store_master as (
    select * from {{ ref('ntaitg_integration__itg_sales_store_master') }}
),
sdl_kr_pos_emart_ssg as (
    select * from {{ ref('ntawks_integration__wks_kr_pos_emart_ssg') }}
),
sdl_kr_ecommerce_naver_sellout as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_naver_sellout') }}
),
sdl_kr_ecom_coupang as (
    select * from {{ ref('ntawks_integration__wks_kr_ecom_coupang') }}
),
sdl_kr_ecommerce_ebay_sellout as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_ebay_sellout') }}
),
sdl_kr_ecommerce_trexi_sellout as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_trexi_sellout') }}
),
sdl_kr_ecommerce_unitoa_sellout as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_unitoa_sellout') }}
),
sdl_kr_ecommerce_tca_sellout as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_tca_sellout') }}
),
sdl_kr_pos_emart_ecvan as (
    select * from {{ ref('ntawks_integration__wks_sdl_kr_pos_emart_ecvan') }}
),
emart as (
    select 
        'Emart' as customer_name,
        master.sold_to as customer_code,
        'SSG.COM' as sub_customer_name,
        nvl(ean, offline_ean) as ean_number,
        null as sap_code,
        product_type as sku_type,
        null as brand_categories,
        cast(prod_nm as varchar(100)) as product_name,
        substring(pos_dt, 0, 4) as year,
        substring(pos_dt, 5, 2) as month,
        null as week,
        to_date(pos_dt, 'YYYYMMDD') as transaction_date,
        cast(sellout_qty as numeric) as sellout_qty,
        cast(replace(sellout_amt, ',', '') as numeric(20, 4)) as sellout_amount,
        null as sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crtd_dttm as crt_dttm,
        filename as source_file_name
    from sdl_kr_pos_emart_ssg as sales,
        itg_sales_store_master as master
    where master.cust_store_cd(+) = sales.str_cd
),
naver_sellout as (
    SELECT 
        cust_nm as customer_name,
        cust_code as customer_code,
        sub_cust_nm as sub_customer_name,
        ean_num as ean_number,
        null as sap_code,
        null as sku_type,
        brand as brand_categories,
        prod_desc as product_name,
        year,
        month,
        week,
        to_date(transaction_date, 'MM/DD/YYYY') as transaction_date,
        sellout_qty,
        sellout_amount,
        null as sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crtd_dttm as crt_dttm,
        file_name as source_file_name
    FROM sdl_kr_ecommerce_naver_sellout
),
coupang as (
    select 
        cust_nm as customer_name,
        cust_code as customer_code,
        sub_cust_nm as sub_customer_name,
        ean_num as ean_number,
        null as sap_code,
        null as sku_type,
        brand as brand_categories,
        prod_desc as product_name,
        null as year,
        null as month,
        null as week,
        coalesce(try_to_date(transaction_date, 'YYYYMMDD'), to_date(transaction_date, 'DD/MM/YY')) as transaction_date,
        sellout_qty,
        sellout_amount,
        null as sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crtd_dttm as crt_dttm,
        file_name as source_file_name
    from sdl_kr_ecom_coupang
    where transaction_date != '' and transaction_date is not null
),
ebay_sellout as (
    select 
        cust_nm as customer_name,
        cust_code as customer_code,
        sub_cust_nm as sub_customer_name,
        ean_num as ean_number,
        sap_cd as sap_code,
        sku_type as sku_type,
        brand as brand_categories,
        prod_desc as product_name,
        year,
        month,
        week,
        to_date(transaction_date, 'YYYYMMDD') as transaction_date,
        sellout_qty,
        sellout_amount,
        sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crtd_dttm as crt_dttm,
        file_name as source_file_name
    from sdl_kr_ecommerce_ebay_sellout
),
trexi_sellout as (
    select 
        cust_nm as customer_name,
        cust_code as customer_code,
        sub_cust_nm as sub_customer_name,
        ean_num as ean_number,
        sap_cd as sap_code,
        sku_type as sku_type,
        brand as brand_categories,
        prod_desc as product_name,
        year,
        month,
        week,
        to_date(transaction_date, 'YYYYMMDD') as transaction_date,
        coalesce(sellout_qty, 0) as sellout_qty,
        coalesce(sellout_amount, 0) as sellout_amount,
        null as sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crtd_dttm as crt_dttm,
        file_name as source_file_name
    from sdl_kr_ecommerce_trexi_sellout

),
unitoa_sellout as (
    select 
        customer_name,
        customer_code,
        sub_customer_name,
        ean_number,
        sap_code,
        sku_type,
        brand_categories,
        product_name,
        year,
        month,
        week,
        to_date(transaction_date) as transaction_date,
        sellout_qty,
        sellout_amount,
        sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crt_dttm,
        source_file_name
    from sdl_kr_ecommerce_unitoa_sellout
),
tca_sellout as (
    select 
        customer_name,
        customer_code,
        sub_customer_name,
        ean_number,
        sap_code,
        sku_type,
        brand_categories,
        product_name,
        year,
        month,
        week,
        to_date(transaction_date) as transaction_date,
        sellout_qty,
        sellout_amount,
        sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crt_dttm,
        source_file_name
    from sdl_kr_ecommerce_tca_sellout
),
ecvan as (
    select 'Emart' as customer_name,
        master.sold_to as customer_code,
        'ECVAN' as sub_customer_name,
        ean as ean_number,
        null as sap_code,
        null as sku_type,
        null as brand_categories,
        cast(prod_nm as varchar(100)) as product_name,
        substring(pos_dt, 0, 4) as year,
        substring(pos_dt, 6, 2) as month,
        null as week,
        cast(pos_dt as date) as transaction_date,
        cast(sellout_qty as numeric) as sellout_qty,
        cast(replace(sellout_amt, ',', '') as numeric(20, 4)) as sellout_amount,
        null as sold_to,
        'KR' as ctry_cd,
        'KRW' as crncy_cd,
        crtd_dttm AS crt_dttm,
        filename AS source_file_name
    from sdl_kr_pos_emart_ecvan as sales,
        itg_sales_store_master as master
    where master.cust_store_cd(+) = sales.str_cd
        and master.store_nm(+) = sales.str_nm
),
transformed as (
    select * from emart
    union all
    select * from naver_sellout
    union all
    select * from coupang
    union all
    select * from ebay_sellout
    union all
    select * from trexi_sellout
    union all
    select * from unitoa_sellout
    union all
    select * from tca_sellout
    union all
    select * from ecvan
),
final as (
    select
        customer_name::varchar(100) as customer_name,
        customer_code::varchar(20) as customer_code,
        sub_customer_name::varchar(100) as sub_customer_name,
        ean_number::varchar(20) as ean_number,
        sap_code::varchar(255) as sap_code,
        sku_type::varchar(20) as sku_type,
        brand_categories::varchar(255) as brand_categories,
        product_name::varchar(100) as product_name,
        year::varchar(20) as year,
        month::varchar(20) as month,
        week::varchar(20) as week,
        transaction_date::date as transaction_date,
        sellout_qty::float as sellout_qty,
        sellout_amount::float as sellout_amount,
        sold_to::varchar(255) as sold_to,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        source_file_name::varchar(255) as source_file_name
    from transformed
)
select * from final