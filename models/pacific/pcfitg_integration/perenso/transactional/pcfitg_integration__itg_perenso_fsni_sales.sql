{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id', 'salesrep_id', 'outlet_id', 'visit_date', 'checkin_time', 'ordervisit'],
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_fsni_sales') }}
),
final as(
    select
        site::varchar(100) as site,
        vendor_id::varchar(20) as vendor_id,
        vendor_name::varchar(100) as vendor_name,
        department::varchar(50) as department,
        merchandise_group::varchar(50) as merchandise_group,
        article::varchar(30) as article,
        article_description::varchar(100) as article_description,
        retail_barcode::varchar(30) as prod_ean,
        banner::varchar(50) as banner,
        store_number::varchar(20) as store_cd,
        store_name::varchar(100),
        billing_document::varchar(30) as bill_doc,
        to_date(billing_date, 'DD.MM.YYYY') as bill_dt,
        to_date(pricing_date, 'DD.MM.YYYY') as pricing_dt,
        net_sales::number(10,2) as sls_value,
        qty_in_sales::number(18,0) as sls_qty,
        sales_uom::varchar(10) as sales_uom,
        qty_in_base::number(18,0) as base_qty,
        base_uom::varchar(10) as base_uom,
        'nzd'::varchar(3) as crncy,
        file_name::varchar(100) as file_name,
        current_timestamp()::timestamp_ntz(9) as crt_dt,
        current_timestamp()::timestamp_ntz(9) as updt_dt
    from source
)
select * from final