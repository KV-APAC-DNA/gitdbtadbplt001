{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trx_date']
    )
}}

--import CTE
with source as (
    select *,dense_rank() over(partition by trx_date order by file_name desc) as rnk 
    from {{ source('sgpsdl_raw','sdl_sg_scan_data_ntuc') }}
    where file_name not in 
    (
        select distinct file_name from 
        {{ source('sgpwks_integration', 'TRATBL_sdl_sg_scan_data_ntuc__null_test') }}
    )
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
    select
        trx_date::date as trx_date,
        left(trx_date, 4)::varchar(20) as year,
        cal.cal_mo_1::varchar(23) as mnth_id,
        cast(left(trx_date, 4)
        || lpad(substring(attribute2, 7), 2, 0) as varchar(20)) as week,
        vendor_code::varchar(100) as vendor_code,
        vendor_name::varchar(255) as vendor_name,
        dept_code::varchar(50) as dept_code,
        dept_description::varchar(255) as dept_description,
        class_no::varchar(50) as class_no,
        class_description::varchar(255) as class_description,
        sub_class_description::varchar(255) as sub_class_description,
        mch::varchar(255) as mch,
        item_code::varchar(100) as item_code,
        item_desc::varchar(500) as item_desc,
        brand::varchar(255) as brand,
        sales_uom::varchar(100) as sales_uom,
        pack_size::number(10, 0) as pack_size,
        store_code::varchar(100) as store_code,
        store_name::varchar(255) as store_name,
        'NA'::varchar(255) as barcode,
        store_format::varchar(255) as store_format,
        attribute2::varchar(50) as attribute2,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name,
        run_id::number(14, 0) as run_id,
        cust_name::varchar(20) as cust_name,
        sum(net_sales)::number(10, 4) as net_sales,
        sum(sales_qty)::number(10, 0) as sales_qty
    from source as sg_ntuc
    left join edw_calendar_dim as cal
        on sg_ntuc.trx_date = cal.cal_day
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28
)

select * from final
