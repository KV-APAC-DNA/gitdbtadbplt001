{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trx_date']
    )
}}

--import CTE
with source as (
    select * from {{ source('sgpsdl_raw','sdl_sg_scan_data_dfi') }}
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
    select
        trxdate::date as trx_date,
        left(trx_date, 4)::varchar(20) as year,
        cal.cal_mo_1::varchar(23) as mnth_id,
        cal.cal_wk::varchar(20) as week,
        buyercode::varchar(50) as buyer_code,
        vendorcode::varchar(100) as vendor_code,
        storecode::varchar(50) as store_code,
        storeshortcode::varchar(100) as store_short_code,
        storedesc::varchar(300) as store_desc,
        brand::varchar(300) as brand,
        itemcode::varchar(50) as item_code,
        supplieritemcode::varchar(50) as supplier_item_code,
        itemdesc::varchar(500) as item_desc,
        size::varchar(100) as size,
        uom::varchar(20) as uom,
        puf::number(10,0) as puf,
        case
            when sg_dfi.barcode is null
                then 'NA'
            else split_part(sg_dfi.barcode, '~', 1)
        end::varchar(255) as barcode,
        cast(salesamount as decimal(14, 4)) as sales_amount,
        cast(salesqty as decimal(10)) as sales_qty,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name,
        run_id::number(14,0) as run_id,
        cust_name::varchar(20) as cust_name
    from source as sg_dfi
    left join edw_calendar_dim as cal
        on sg_dfi.trxdate = cal.cal_day

)

--final select
select * from final
