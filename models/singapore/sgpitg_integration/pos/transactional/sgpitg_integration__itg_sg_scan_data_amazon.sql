{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['mnth_id']
    )
}}

--import CTE
with source as (
    select * 
    from {{ source('sgpsdl_raw','sdl_sg_scan_data_amazon') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_sg_scan_data_amazon__null_test') }}
    ) qualify rnk = 1
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
    select
    evotd.week_date::date as trx_date,
    sg_amazon.year::varchar(20) as year,
    evotd.cal_mo_1::varchar(23) as mnth_id,
    null::varchar(20) as week,
    rm::varchar(50) as rm,
    merchant_customer_id::varchar(15) as merchant_customer_id,
    gl::varchar(50) as gl,
    category::varchar(200) as category,
    subcategory::varchar(200) as subcategory,
    brand::varchar(255) as brand,
    item_code::varchar(50) as item_code,
    item_desc::varchar(500) as item_desc,
    net_sales::number(10, 4) as net_sales,
    pcogs::number(10, 4) as pcogs,
    sales_qty::number(10, 0) as sales_qty,
    ppmpercent::number(10, 5) as ppmpercent,
    ppmdollar::number(10, 5) as ppmdollar,
    month::number(18, 0) as month,
    vendor_code::varchar(10) as vendor_code,
    vendor_name::varchar(255) as vendor_name,
    store::varchar(50) as store,
    store_name::varchar(50) as store_name,
    'NA'::varchar(255) as barcode,
    cdl_dttm::varchar(255) as cdl_dttm,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name,
    run_id::number(14, 0) as run_id
    from source as sg_amazon
    inner join (
        select distinct
            cal_mo_1,
            cal_yr || lpad(cal_wk, 2, 0) as week,
            min(cal_day) as week_date
        from edw_calendar_dim
        group by
            1,
            2
    ) as evotd
        on sg_amazon.year || lpad(month, 2, 0) = evotd.cal_mo_1
)

--final select
select * from final
