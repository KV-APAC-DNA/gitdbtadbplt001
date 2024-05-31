{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where to_date(nvl(transaction_date, '9999-12-31')) || ean in (select distinct nvl(transaction_date, '9999-12-31') || ean_number from ntaitg_integration.itg_kr_ecommerce_sellout where upper(customer_name) = 'EMART' AND UPPER(SUB_CUSTOMER_NAME) = 'SSG.COM' and upper(ctry_cd) = 'KR' ) and upper(retailer_code) = 'SSG.COM';
        delete from {{this}} where to_date(TRANSACTION_DATE) || EAN IN (SELECT DISTINCT TRANSACTION_DATE || EAN_NUMBER FROM ntaitg_integration.itg_kr_ecommerce_sellout where upper(customer_name) = 'NAVER' and upper(ctry_cd) = 'KR' ) AND RETAILER_CODE = '136250';
        delete from {{this}} where to_date(TRANSACTION_DATE) || EAN IN (SELECT DISTINCT TRANSACTION_DATE || EAN_NUMBER FROM ntaitg_integration.itg_kr_ecommerce_sellout where upper(customer_name) = '(JU) UNITOA_COUPANG' and upper(ctry_cd) = 'KR' ) AND RETAILER_CODE = '140555';
        delete from {{this}} where to_date(transaction_date) || ean in (select distinct transaction_date || ean_number from ntaitg_integration.itg_kr_ecommerce_sellout where upper(customer_name) = 'EBAY' and upper(ctry_cd) = 'KR' ) and upper(retailer_code) = 'EBAY';
        delete from {{this}} where (to_date(transaction_date) || ean) in (select distinct (transaction_date || ean_number) from ntaitg_integration.itg_kr_ecommerce_sellout where upper(customer_name) = 'TREXI' and upper(ctry_cd) = 'KR' ) and upper(retailer_code) = 'TREXI';
        delete from {{this}} where source_file_name = (select distinct source_file_name from {{ ref('ntawks_integration__wks_kr_ecommerce_offtake_coupang_transaction') }});
        delete from {{this}} where source_file_name = (select distinct source_file_name from {{ ref('ntawks_integration__wks_kr_ecommerce_offtake_sales_ebay') }});
        {% endif %}"
    )
}}
with itg_kr_ecommerce_sellout as (
    select * from ntaitg_integration.itg_kr_ecommerce_sellout
),
sdl_kr_ecommerce_offtake_sales_ebay as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_offtake_sales_ebay') }}
),
sdl_kr_ecommerce_offtake_coupang_transaction as (
    select * from {{ ref('ntawks_integration__wks_kr_ecommerce_offtake_coupang_transaction') }}
),
itg_kr_ecommerce_offtake_product_master as (
    select * from ntaitg_integration.itg_kr_ecommerce_offtake_product_master
),
itg_kr_ecommerce_offtake_sales_ebay as (
    select * from ntaitg_integration.itg_kr_ecommerce_offtake_sales_ebay
),
itg_kr_ecommerce_offtake_coupang_transaction as (
    select * from ntaitg_integration.itg_kr_ecommerce_offtake_coupang_transaction
),
edw_retailer_mapping as (
    select * from {{ source('aspedw_integration', 'edw_retailer_mapping') }}
),
emart as (
    SELECT 
        CRT_DTTM AS LOAD_DATE,
        SOURCE_FILE_NAME,
        '#N/A' AS RETAILER_SKU_CODE,
        NULL AS NO_OF_UNITS_SOLD,
        CASE
            WHEN EAN_NUMBER IS NULL THEN '#N/A'
            ELSE EAN_NUMBER
        END AS EAN,
        NULL AS ORDER_DATE,
        SELLOUT_AMOUNT AS SALES_VALUE,
        SELLOUT_QTY AS QUANTITY,
        'SSG.COM' AS RETAILER_CODE,
        'SSG.COM' AS RETAILER_NAME,
        nvl(TRANSACTION_DATE, '9999-12-31') as TRANSACTION_DATE,
        '#N/A' AS ORDER_NUMBER,
        NULL as PRODUCT_CODE,
        PRODUCT_NAME as PRODUCT_TITLE,
        CRNCY_CD as TRANSACTION_CURRENCY,
        'KOREA' as COUNTRY,
        SUB_CUSTOMER_NAME
    FROM ITG_KR_ECOMMERCE_SELLOUT
    WHERE UPPER(CUSTOMER_NAME) = 'EMART'
        AND UPPER(SUB_CUSTOMER_NAME) = 'SSG.COM'
        AND UPPER(CTRY_CD) = 'KR'
),
naver as (
    SELECT 
        CRT_DTTM AS LOAD_DATE,
        SOURCE_FILE_NAME,
        '#N/A' AS RETAILER_SKU_CODE,
        NULL AS NO_OF_UNITS_SOLD,
        CASE
            WHEN EAN_NUMBER IS NULL THEN '#N/A'
            ELSE EAN_NUMBER
        END AS EAN,
        NULL AS ORDER_DATE,
        SELLOUT_AMOUNT AS SALES_VALUE,
        SELLOUT_QTY AS QUANTITY,
        '136250' AS RETAILER_CODE,
        'NAVER' AS RETAILER_NAME,
        TRANSACTION_DATE,
        '#N/A' AS ORDER_NUMBER,
        NULL AS PRODUCT_CODE,
        PRODUCT_NAME AS PRODUCT_TITLE,
        CRNCY_CD AS TRANSACTION_CURRENCY,
        'KOREA' AS COUNTRY,
        null as SUB_CUSTOMER_NAME
    FROM ITG_KR_ECOMMERCE_SELLOUT
    WHERE UPPER(CUSTOMER_NAME) = 'NAVER'
        AND UPPER(CTRY_CD) = 'KR'
),
unitoa_coupang as (
    SELECT 
        CRT_DTTM AS LOAD_DATE,
        SOURCE_FILE_NAME,
        '#N/A' AS RETAILER_SKU_CODE,
        NULL AS NO_OF_UNITS_SOLD,
        CASE
            WHEN EAN_NUMBER IS NULL THEN '#N/A'
            ELSE EAN_NUMBER
        END AS EAN,
        NULL AS ORDER_DATE,
        SELLOUT_AMOUNT AS SALES_VALUE,
        SELLOUT_QTY AS QUANTITY,
        '140555' AS RETAILER_CODE,
        '(JU) UNITOA_COUPANG' AS RETAILER_NAME,
        TRANSACTION_DATE,
        '#N/A' AS ORDER_NUMBER,
        NULL AS PRODUCT_CODE,
        PRODUCT_NAME AS PRODUCT_TITLE,
        CRNCY_CD AS TRANSACTION_CURRENCY,
        'KOREA' AS COUNTRY,
        null as SUB_CUSTOMER_NAME
    FROM ITG_KR_ECOMMERCE_SELLOUT
    WHERE UPPER(CUSTOMER_NAME) = '(JU) UNITOA_COUPANG'
        AND UPPER(CTRY_CD) = 'KR'
),
ebay as (
    SELECT 
        CRT_DTTM AS LOAD_DATE,
        SOURCE_FILE_NAME,
        '#N/A' AS RETAILER_SKU_CODE,
        NULL AS NO_OF_UNITS_SOLD,
        CASE
            WHEN EAN_NUMBER IS NULL THEN '#N/A'
            ELSE EAN_NUMBER
        END AS EAN,
        NULL AS ORDER_DATE,
        SELLOUT_AMOUNT AS SALES_VALUE,
        SELLOUT_QTY AS QUANTITY,
        'eBay' AS RETAILER_CODE,
        'eBay' AS RETAILER_NAME,
        TRANSACTION_DATE,
        '#N/A' AS ORDER_NUMBER,
        NULL AS PRODUCT_CODE,
        PRODUCT_NAME AS PRODUCT_TITLE,
        CRNCY_CD AS TRANSACTION_CURRENCY,
        'KOREA' AS COUNTRY,
        null as SUB_CUSTOMER_NAME
    FROM ITG_KR_ECOMMERCE_SELLOUT
    WHERE UPPER(CUSTOMER_NAME) = 'EBAY'
        AND UPPER(CTRY_CD) = 'KR'
),
trexi as (
    SELECT 
        CRT_DTTM AS LOAD_DATE,
        SOURCE_FILE_NAME,
        '#N/A' AS RETAILER_SKU_CODE,
        NULL AS NO_OF_UNITS_SOLD,
        CASE
            WHEN EAN_NUMBER IS NULL THEN '#N/A'
            ELSE EAN_NUMBER
        END AS EAN,
        NULL AS ORDER_DATE,
        COALESCE(SELLOUT_AMOUNT, 0) AS SALES_VALUE,
        COALESCE(SELLOUT_QTY, 0) AS QUANTITY,
        UPPER(CUSTOMER_NAME) AS RETAILER_CODE,
        UPPER(CUSTOMER_NAME) AS RETAILER_NAME,
        TRANSACTION_DATE,
        '#N/A' AS ORDER_NUMBER,
        SAP_CODE AS PRODUCT_CODE,
        PRODUCT_NAME AS PRODUCT_TITLE,
        CRNCY_CD AS TRANSACTION_CURRENCY,
        'KOREA' AS COUNTRY,
        SUB_CUSTOMER_NAME
    FROM ITG_KR_ECOMMERCE_SELLOUT
    WHERE UPPER(CUSTOMER_NAME) = 'TREXI'
        AND UPPER(CTRY_CD) = 'KR'
),
sales_ebay as (
    select
        itg_sales.load_date,
        itg_sales.source_file_name,
        itg_sales.sku_code as retailer_sku_code,
        itg_sales.no_of_units_sold,
        case when prod_dim.ean is null then '#N/A' else prod_dim.ean end as ean,
        itg_sales.order_date,
        itg_sales.sales_value,
        itg_sales.quantity,
        itg_sales.retailer_code,
        case when mapping.retailer_cd = itg_sales.retailer_code then mapping.retailer_nm else itg_sales.retailer_name end as retailer_name,
        itg_sales.transaction_date,
        itg_sales.order_number,
        itg_sales.product_code,
        itg_sales.product_title,
        itg_sales.transaction_currency,
        itg_sales.country,
        null as sub_customer_name
    from itg_kr_ecommerce_offtake_sales_ebay itg_sales
    left join itg_kr_ecommerce_offtake_product_master prod_dim on itg_sales.sku_code = prod_dim.retailer_sku_code
    left join edw_retailer_mapping mapping on mapping.retailer_cd = itg_sales.retailer_code
    where itg_sales.load_date = (select max(load_date) from sdl_kr_ecommerce_offtake_sales_ebay)
),
sales_copang as (
    select
        itg_sales_copang.load_date,
        itg_sales_copang.source_file_name,
        COALESCE(itg_sales_copang.sku_code,'#N/A') as retailer_sku_code,
        NULL AS no_of_units_sold,
        case when prod_dim.ean is null then '#N/A' else prod_dim.ean end as ean,
        NULL AS order_date,
        itg_sales_copang.sales_value,
        itg_sales_copang.quantity,
        'Coupang' AS retailer_code,
        'Coupang' AS retailer_name,
        itg_sales_copang.transaction_date,
        '#N/A' AS order_number,
        itg_sales_copang.product_id as product_code,
        itg_sales_copang.sku_name as product_title,
        itg_sales_copang.transaction_currency,
        itg_sales_copang.country,
        null as sub_customer_name
    from itg_kr_ecommerce_offtake_coupang_transaction itg_sales_copang
    left join itg_kr_ecommerce_offtake_product_master prod_dim on itg_sales_copang.sku_code = prod_dim.retailer_sku_code
    where itg_sales_copang.load_date = (select max(load_date) from sdl_kr_ecommerce_offtake_coupang_transaction)
),
transformed as (
    select * from emart
    union all
    select * from naver
    union all
    select * from unitoa_coupang
    union all
    select * from ebay
    union all
    select * from trexi
    union all
    select * from sales_ebay
    union all
    select * from sales_copang
),
final as (
    select
        load_date::timestamp_ntz(9) as load_date,
        source_file_name::varchar(255) as source_file_name,
        retailer_sku_code::varchar(20) as retailer_sku_code,
        no_of_units_sold::number(18,0) as no_of_units_sold,
        ean::varchar(20) as ean,
        order_date::timestamp_ntz(9) as order_date,
        sales_value::float as sales_value,
        quantity::number(18,0) as quantity,
        retailer_code::varchar(2000) as retailer_code,
        retailer_name::varchar(2000) as retailer_name,
        transaction_date::timestamp_ntz(9) as transaction_date,
        order_number::varchar(255) as order_number,
        product_code::varchar(255) as product_code,
        product_title::varchar(20000) as product_title,
        transaction_currency::varchar(10) as transaction_currency,
        country::varchar(10) as country,
        sub_customer_name::varchar(255) as sub_customer_name
    from transformed
)
select * from final