with sdl_ecommerce_offtake_amazon as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_amazon') }}
),
sdl_ecommerce_offtake_bigbasket as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_bigbasket') }}
),
sdl_ecommerce_offtake_grofers as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_grofers') }}
),
sdl_ecommerce_offtake_nykaa as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_nykaa') }}
),
sdl_ecommerce_offtake_firstcry as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_firstcry') }}
),
sdl_ecommerce_offtake_paytm as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_paytm') }}
),
itg_ecommerce_offtake_amazon as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_amazon') }}
),
itg_ecommerce_offtake_flipkart as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_flipkart') }}
),
itg_ecommerce_offtake_bigbasket as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_bigbasket') }}
),
itg_ecommerce_offtake_firstcry as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_firstcry') }}
),
itg_ecommerce_offtake_grofers as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_grofers') }}
),
itg_ecommerce_offtake_nykaa as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_nykaa') }}
),
itg_ecommerce_offtake_paytm as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_paytm') }}
),
itg_ecommerce_offtake_master_mapping as
(
    select * from {{ ref('inditg_integration__itg_ecommerce_offtake_master_mapping') }}
),
amazon as
(
    select to_date(('01' || right(itg_amazon.source_file_name,5)),'DDMonYY') as transaction_date,
    'India' as country,
    'Amazon' as platform,
    'Amazon' as account_name,
    itg_amazon.rpc as account_sku_code,
    itg_amazon.product_title as retailer_product_name,
    itg_amazon.brand as retailer_brand,
    case when mapping.ean is null then '#N/A' else mapping.ean end as ean,
    itg_amazon.quantity as sales_qty,
    itg_amazon.mrp_offtakes_value as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    mapping.lakshya_sku_name as generic_product_code
    from itg_ecommerce_offtake_amazon itg_amazon left join itg_ecommerce_offtake_master_mapping mapping 
    on itg_amazon.rpc = mapping.account_sku_code 
    where itg_amazon.month = (select max(month) from sdl_ecommerce_offtake_amazon)
),
bigbasket as
(
    select to_date(('01' || right(itg_bigbasket.source_file_name,5)),'DDMonYY') as transaction_date,
    'India' as country,
    'Bigbasket' as platform,
    'Bigbasket' as account_name,
    itg_bigbasket.product_id as account_sku_code,
    itg_bigbasket.product_description as retailer_product_name,
    itg_bigbasket.brand_name as retailer_brand,
    case when mapping.ean is null then '#N/A' else mapping.ean end as ean,
    itg_bigbasket.qty_sold as sales_qty,
    itg_bigbasket.total_sales as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    mapping.lakshya_sku_name as generic_product_code 
    from itg_ecommerce_offtake_bigbasket itg_bigbasket left join itg_ecommerce_offtake_master_mapping mapping 
    on itg_bigbasket.product_id = mapping.account_sku_code 
    where itg_bigbasket.source_file_name = (select distinct source_file_name from sdl_ecommerce_offtake_bigbasket)
),
firstcry as
(
    select to_date(('01' || right(itg_firstcry.source_file_name,5)),'DDMonYY') as transaction_date,
    'India' as country,
    'FirstCry' as platform,
    'FirstCry' as account_name,
    itg_firstcry.product_id as account_sku_code,
    itg_firstcry.product_name as retailer_product_name,
    itg_firstcry.brand_name as retailer_brand,
    case when mapping.ean is null then '#N/A' else mapping.ean end as ean,
    itg_firstcry.sum_of_sales as sales_qty,
    itg_firstcry.sum_of_mrpsales as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    mapping.lakshya_sku_name as generic_product_code
    from itg_ecommerce_offtake_firstcry itg_firstcry left join itg_ecommerce_offtake_master_mapping mapping 
    on itg_firstcry.product_id = mapping.account_sku_code 
    where itg_firstcry.source_file_name = (select distinct source_file_name from sdl_ecommerce_offtake_firstcry)
),
grofers as
(
    select to_date(('01' || right(itg_grofers.source_file_name,5)),'DDMonYY') as transaction_date,
    'India' as country,
    'Grofers' as platform,
    'Grofers' as account_name,
    itg_grofers.product_id as account_sku_code,
    itg_grofers.product_name as retailer_product_name,
    null as retailer_brand,
    case when mapping.ean is null then '#N/A' else mapping.ean end as ean,
    itg_grofers.sum_of_qty_sold as sales_qty,
    itg_grofers.sum_of_mrp_gmv as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    mapping.lakshya_sku_name as generic_product_code
    from itg_ecommerce_offtake_grofers itg_grofers left join itg_ecommerce_offtake_master_mapping mapping 
    on itg_grofers.product_id = mapping.account_sku_code 
    where l_cat not like '%Total%' and l1_cat not like '%Total%' and itg_grofers.source_file_name = (select distinct source_file_name from sdl_ecommerce_offtake_grofers) 
),
nykaa as
(
    select to_date(('01' || right(itg_nykaa.source_file_name,5)),'DDMonYY') as transaction_date,
    'India' as country,
    'Nykaa' as platform,
    'Nykaa' as account_name,
    itg_nykaa.sku_code as account_sku_code,
    itg_nykaa.product_name as retailer_product_name,
    null as retailer_brand,
    case when mapping.ean is null then '#N/A' else mapping.ean end as ean,
    itg_nykaa.qty as sales_qty,
    itg_nykaa.mrp as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    mapping.lakshya_sku_name as generic_product_code
    from itg_ecommerce_offtake_nykaa itg_nykaa left join itg_ecommerce_offtake_master_mapping mapping 
    on itg_nykaa.sku_code = mapping.account_sku_code 
    where itg_nykaa.load_date = (select max(load_date) from sdl_ecommerce_offtake_nykaa)
),
paytm as
(
    select to_date(date) as transaction_date,
    'India' as country,
    'Paytm' as platform,
    'Paytm' as account_name,
    itg_paytm.product_id as account_sku_code,
    itg_paytm.product_name as retailer_product_name,
    itg_paytm.brand_name as retailer_brand,
    case when mapping.ean is null then '#N/A' else mapping.ean end as ean,
    itg_paytm.quantity_ordered as sales_qty,
    itg_paytm.gmv_ordered as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    mapping.lakshya_sku_name as generic_product_code
    from itg_ecommerce_offtake_paytm itg_paytm left join itg_ecommerce_offtake_master_mapping mapping 
    on itg_paytm.product_id = mapping.account_sku_code 
    where itg_paytm.load_date = (select max(load_date) from sdl_ecommerce_offtake_paytm) 
),
flipkart as
(
    select decode(txn.transaction_date,null,cast((extract(year from cast(txn.load_date as date))||'-'||extract(month from cast(txn.load_date as date))||'-'||'15') as date),txn.transaction_date) as transaction_date,
    'India' as country,
    txn.account_name as platform,
    txn.account_name,
    txn.fsn as account_sku_code,
    txn.product_description as retailer_product_name,
    txn.brand as retailer_brand,
    decode(prod_map.ean,null,'#N/A',ean) as ean,
    txn.qty as sales_qty,
    txn.gmv as sales_value,
    'INR' as transaction_currency,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    prod_map.lakshya_sku_name as generic_product_code 
    from itg_ecommerce_offtake_flipkart txn left join itg_ecommerce_offtake_master_mapping prod_map 
    on txn.fsn=prod_map.account_sku_code and txn.account_name=prod_map.account_name
),
transformed as 
(
    select * from amazon
    union all
    select * from bigbasket
    union all
    select * from firstcry
    union all
    select * from grofers
    union all
    select * from nykaa
    union all
    select * from paytm
    union all
    select * from flipkart
),
final as 
(   
    select
        transaction_date::varchar(20) as transaction_date,
        country::varchar(255) as country,
        platform::varchar(255) as platform,
        account_name::varchar(255) as account_name,
        account_sku_code::varchar(255) as account_sku_code,
        retailer_product_name::varchar(65535) as retailer_product_name,
        retailer_brand::varchar(2000) as retailer_brand,
        ean::varchar(255) as ean,
        sales_qty::float as sales_qty,
        sales_value::float as sales_value,
        transaction_currency::varchar(20) as transaction_currency,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        generic_product_code::varchar(255) as generic_product_code
    from transformed 
)
select * from final
