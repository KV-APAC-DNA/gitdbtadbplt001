--Import CTE
with sdl_mds_in_ecom_product as (
    select * from {{ source('indsdl_raw', 'sdl_mds_in_ecom_product') }}
),

sdl_mds_cn_ecom_product as (
    select * from {{ source('chnsdl_raw', 'sdl_mds_cn_ecom_product') }}
),

sdl_mds_jp_ecom_product as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_ecom_product') }}
),

sdl_mds_jp_dcl_ecom_product as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_dcl_ecom_product') }}
),

sdl_mds_pacific_ecom_product as (
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ecom_product') }}
),

sdl_mds_hk_ecom_product as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_ecom_product') }}
),

sdl_mds_tw_ecom_product as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_ecom_product') }}
),

sdl_kr_ecommerce_offtake_product_master as (
    select *
    from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_offtake_product_master') }}
),

sdl_mds_kr_ecom_offtake_product_mapping as (
    select *
    from {{ source('ntasdl_raw', 'sdl_mds_kr_ecom_offtake_product_mapping') }}
),

sdl_mds_ph_ecom_product as (
    select * from {{ source('osesdl_raw', 'sdl_mds_ph_ecom_product') }}
),

sdl_mds_my_ecom_product as (
    select * from {{ source('osesdl_raw', 'sdl_mds_my_ecom_product') }}
),

sdl_mds_th_ecom_product as (
    select * from {{ source('osesdl_raw', 'sdl_mds_th_ecom_product') }}
),

sdl_mds_sg_ecom_product as (
    select * from {{ source('osesdl_raw', 'sdl_mds_sg_ecom_product') }}
),

sdl_mds_vn_ecom_product as (
    select * from {{ source('osesdl_raw', 'sdl_mds_vn_ecom_product') }}
),

sdl_mds_id_ecom_product as (
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ecom_product') }}
),


--Logical CTE
transformed as (
    select
        'India' as "market",
        name as "manufacturer",
        cust_attr_1 as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_in_ecom_product group by 1, 2, 3, 4, 5
    union all
    select
        'China Personal Care' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_cn_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Japan' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_jp_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Japan DCL' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_jp_dcl_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Australia' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_pacific_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Hong Kong' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_hk_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Taiwan' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_tw_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    -- Korea SKU mapping for manual + automated files
    select
        "market",
        "manufacturer",
        "account_name",
        "rpc",
        max(ean) as "ean"
    from (
        -- J&J Korea SKUs from automated file
        select
            'Korea' as "market",
            'J&J' as "manufacturer",
            retailer_type as "account_name",
            retailer_barcode as "rpc",
            ean
        from sdl_kr_ecommerce_offtake_product_master group by 1, 2, 3, 4, 5
        union all
        -- Get Korea Competitor mappings from MDS table + J&J Coupang mappings
        select
            'Korea' as "market",
            case when upper(name) = 'COMPETITOR' then name else 'J&J' end
                as "manufacturer",
            name as "account_name",
            retailer_sku_code as "rpc",
            ean
        from sdl_mds_kr_ecom_offtake_product_mapping
        group by 1, 2, 3, 4, 5
    )
    group by 1, 2, 3, 4
    union all
    select
        'Philippines' as "market",
        name as "manufacturer",
        null as "account_name",
        rpc,
        sku1_code as "ean"
    from sdl_mds_ph_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Malaysia' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_my_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Thailand' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_th_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Singapore' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_sg_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Vietnam' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_vn_ecom_product
    group by 1, 2, 3, 4, 5
    union all
    select
        'Indonesia' as "market",
        name as "manufacturer",
        null as "account_name",
        prod_attr_1 as "rpc",
        upc as "ean"
    from sdl_mds_id_ecom_product
    group by 1, 2, 3, 4, 5
),

--Final CTE
final as (
    select 
        "market"::varchar(510) as market,			
        "manufacturer"::varchar(510) as manufacturer,	 
        "account_name"::varchar(510) as account_name,	 
        "rpc"::varchar(510) as rpc,				 
        "ean"::varchar(510) as ean,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
   from transformed
)

--Final select
select * from final