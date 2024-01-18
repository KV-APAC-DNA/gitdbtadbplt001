{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trx_date']
    )
}}

--import CTE
with source as (
    select * from {{ source('snaposesdl_raw','sdl_sg_scan_data_guardian') }}
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
    select
        trxdate::date as trx_date,
        left(trxdate, 4)::varchar(20) as year,
        cal.cal_mo_1::varchar(23) as mnth_id,
        cal.cal_wk::varchar(20) as week,
        buyercode::varchar(50) as buyer_code,
        vendorcode::varchar(100) as vendor_code,
        storecode::varchar(50) as store_code,
        storeshortcode::varchar(50) as store_short_code,
        storepostalcode::varchar(50) as store_postal_code,
        storeaddress1::varchar(200) as store_address_1,
        storeaddress2::varchar(200) as store_address_2,
        storeaddress3::varchar(100) as store_address_3,
        storecountry::varchar(20) as store_country,
        storedesc::varchar(500) as store_desc,
        brand::varchar(300) as brand,
        itemcode::varchar(50) as item_code,
        supplieritemcode::varchar(50) as supplier_item_code,
        itemdesc::varchar(500) as item_desc,
        size::varchar(100) as size,
        uom::varchar(20) as uom,
        puf::number(10,0) as puf,
        cast(salesqty as decimal(10)) as sales_qty,
        cast(salesamount as decimal(14, 4)) as sales_amount,
        inventoryonhand::NUMBER(10,0) as inventory_on_hand,
        case when sg_grdn.barcode is null then 'NA' else sg_grdn.barcode end::varchar(255) as barcode,
        cdl_dttm::varchar(255) as ascdl_dttm ,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name,
        run_id::number(14,0) as run_id,
        cust_name::varchar(20) as cust_name
    from source as sg_grdn
    left join edw_calendar_dim as cal
        on sg_grdn.trxdate = cal.cal_day
)

--final select
select * from final
