with sdl_kr_pos_eland as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_eland') }}
),
sdl_kr_pos_lotte_mart as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_lotte_mart') }}
),
sdl_kr_pos_homeplus_online as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_homeplus_online') }}
),
sdl_kr_pos_lohbs as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_lohbs') }}
),
sdl_kr_pos_homeplus as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_homeplus') }}
),
sdl_kr_pos_gs_super as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_gs_super') }}
),
sdl_kr_pos_lotte_super as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_lotte_super') }}
),
sdl_kr_pos_olive_young as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_olive_young') }}
),
sdl_kr_pos_lalavla as 
(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_lalavla') }}
),
eland as 
(
    select 
        barcode,
        Null as customer_code,
        Null as customer_code_vend_cd,
        null as date_of_preparation,
        null as distribution_code,
        number_of_sales,
        pos_date,
        null as product_code,
        product_name,
        null as product_status,
        null as product_volume,
        sales_rvenue_incl_vat as sales_revenue,
        sales_rvenue_excl_vat,
        null as serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        store_name,
        null as unit_price,
        currency,
        crt_dttm,
        upd_dttm,
        'E-Land Retail' as src_sys_cd
    from sdl_kr_pos_eland
),
lotte_mart as 
(
    select 
        barcode,
        Null as customer_code,
        Null as customer_code_vend_cd,
        null as date_of_preparation,
        null as distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        product_name,
        null as product_status,
        null as product_volume,
        sales_revenue,
        null as sales_rvenue_excl_vat,
        null as serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        store_name,
        null as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Lotte mart' as src_sys_cd
    from sdl_kr_pos_lotte_mart
),
homeplus_online as 
(
    select 
        bar_code as barcode,
        null as customer_code,
        Null as customer_code_vend_cd,
        null as date_of_preparation,
        null as distribution_code,
        number_of_sales,
        pos_date,
        null as product_code,
        null as product_name,
        null as product_status,
        null as product_volume,
        sales_revenue::numeric(16, 5) as sales_revenue,
        null as sales_rvenue_excl_vat,
        null as serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        store_name,
        unit_price::numeric(16, 5) as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Homeplus' as src_sys_cd
    from sdl_kr_pos_homeplus_online
),
lohbs as 
(
    select 
        barcode,
        customer_code,
        Null as customer_code_vend_cd,
        null as date_of_preparation,
        null as distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        product_name,
        null as product_status,
        product_volume,
        sales_revenue,
        null as sales_rvenue_excl_vat,
        null as serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        store_name,
        null as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Lotte_LOHB''s' as src_sys_cd
    from sdl_kr_pos_lohbs
),
homeplus as 
(
    select
        bar_code as barcode,
        customer_code,
        Null as customer_code_vend_cd,
        date_of_preparation,
        distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        null as product_name,
        null as product_status,
        null as product_volume,
        sales_revenue::numeric(16, 5) as sales_revenue,
        null as sales_rvenue_excl_vat,
        serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        null as store_name,
        unit_price::numeric(16, 5) as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Homeplus' as src_sys_cd
    from sdl_kr_pos_homeplus
),
gs_super as 
(
    select 
        bar_code as barcode,
        Null as customer_code,
        customer_code as customer_code_vend_cd,
        date_of_preparation,
        distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        null as product_name,
        null as product_status,
        null as product_volume,
        sales_revenue,
        null as sales_rvenue_excl_vat,
        serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        null as store_name,
        unit_price::numeric(16, 5) as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'GS Chain Super' as src_sys_cd
    from sdl_kr_pos_gs_super
),
lotte_super as 
(
    select 
        barcode,
        customer_code,
        Null as customer_code_vend_cd,
        null as date_of_preparation,
        null as distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        product_name,
        null as product_status,
        product_volume,
        sales_revenue,
        null as sales_rvenue_excl_vat,
        null as serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        store_name,
        null as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Lotte Chain Super' as src_sys_cd
    from sdl_kr_pos_lotte_super
),
olive_young as 
(
    select
        barcode,
        Null as customer_code,
        Null as customer_code_vend_cd,
        null as date_of_preparation,
        null as distribution_code,
        number_of_sales,
        pos_date,
        null as product_code,
        product_name,
        null as product_status,
        null as product_volume,
        sales_revenue,
        null as sales_rvenue_excl_vat,
        null as serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        store_name,
        null as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Olive Young' as src_sys_cd
    from sdl_kr_pos_olive_young
),
lalavla as 
(
    select 
        bar_code as barcode,
        Null as customer_code,
        customer_code as customer_code_vend_cd,
        date_of_preparation,
        distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        null as product_name,
        null as product_status,
        null as product_volume,
        sales_revenue,
        null as sales_rvenue_excl_vat,
        serial_num,
        null as sl_no,
        ltrim(store_code, 0) as store_code,
        null as store_name,
        unit_price::numeric(16, 5) as unit_price,
        'KRW' as currency,
        crt_dttm,
        upd_dttm,
        'Lalavla' as src_sys_cd
    from sdl_kr_pos_lalavla
),
all_retailer as 
(
    select * from eland
    union all
    select * from lotte_mart
    union all
    /*Adding scripts for Korea ecom pos - Homeplus online. Source system code would be the same and only store name would vary appended by _online*/
    select * from homeplus_online
    union all
    select * from lohbs
    union all
    select * from homeplus    
    union all
    select * from gs_super
    union all
    select * from lotte_super
    union all
    select * from olive_young
    union all
    select * from lalavla
),
final as
(
    select 
        barcode,
        customer_code,
        customer_code_vend_cd,
        date_of_preparation,
        distribution_code,
        number_of_sales,
        pos_date,
        product_code,
        product_name,
        product_status,
        product_volume,
        sales_revenue,
        sales_rvenue_excl_vat,
        serial_num,
        sl_no,
        store_code,
        store_name,
        unit_price,
        currency,
        crt_dttm,
        upd_dttm,
        src_sys_cd,
        rank () over (partition by src_sys_cd,pos_date,trim(store_code),trim(barcode) order by upd_dttm desc) as rnk
    from all_retailer
)
select * from final