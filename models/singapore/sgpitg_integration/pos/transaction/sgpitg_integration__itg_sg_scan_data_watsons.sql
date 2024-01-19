{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['week']
    )
}}

--import CTE
with source as (
    select * from {{ source('sgpsdl_raw','sdl_sg_scan_data_watsons') }}
),

edw_vw_os_time_dim as (
    select * from {{ source('oseedw_access','edw_vw_os_time_dim') }}
),

final as (
        select
        ssdw.year::varchar(20) as year,
        evotd.mnth_id::varchar(23) as mnth_id,
        ssdw.week::varchar(20) as week,
        ssdw.store::varchar(255) as store,
        ssdw.div::varchar(255) as div,
        ssdw.prdt_dept::varchar(255) as prdt_dept,
        ssdw.prdtcode::varchar(255) as prdtcode,
        ssdw.prdtdesc::varchar(500) as prdtdesc,
        ssdw.brand::varchar(300) as brand,
        ssdw.supcode::varchar(255) as supcode,
        ssdw.sup_name::varchar(300) as sup_name,
        case when ssdw.barcode is null then 'NA' else ssdw.barcode end::varchar(255) as barcode,
        ssdw.sup_cat::varchar(255) as sup_cat,
        ssdw.dept_name::varchar(255) as dept_name,
        cast(ssdw.net_sales as decimal(14, 4)) as net_sales,
        cast(ssdw.sales_qty as decimal(10)) as sales_qty,
        evotd.week_date::date as bill_date,
        ssdw.cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        ssdw.file_name::varchar(255) as file_name,
        ssdw.run_id::number(14,0) as run_id,
        cust_name::varchar(20) as cust_name
    from source as ssdw
    left join (
        select distinct
            mnth_id,
            "year" || lpad(wk, 2, 0) as week,
            min(cal_date) as week_date
        from edw_vw_os_time_dim
        group by
            1,
            2
    ) as evotd
        on case when ssdw.week like '%53' then left(ssdw.week, 4) || 52 else ssdw.week end = evotd.week
)

select * from final