{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename']
    )
}}
with source as
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellout_aeon') }}
),
transformed as
(
    select 
        substring(right (filename, 11), 4, 4) as year,
        case
            when substring(right (filename, 11), 1, 3) like '%Jan%' then '01'
            when substring(right (filename, 11), 1, 3) like '%Feb%' then '02'
            when substring(right (filename, 11), 1, 3) like '%Mar%' then '03'
            when substring(right (filename, 11), 1, 3) like '%Apr%' then '04'
            when substring(right (filename, 11), 1, 3) like '%May%' then '05'
            when substring(right (filename, 11), 1, 3) like '%Jun%' then '06'
            when substring(right (filename, 11), 1, 3) like '%Jul%' then '07'
            when substring(right (filename, 11), 1, 3) like '%Aug%' then '08'
            when substring(right (filename, 11), 1, 3) like '%Sep%' then '09'
            when substring(right (filename, 11), 1, 3) like '%Oct%' then '10'
            when substring(right (filename, 11), 1, 3) like '%Nov%' then '11'
            when substring(right (filename, 11), 1, 3) like '%Dec%' then '12'
        end AS month,
        store as store,
        department as department,
        supplier_code as supplier_code,
        supplier_name as supplier_name,
        item as item,
        item_name as item_name,
        sales_quantity as sales_quantity,
        sales_amount as sales_amount,
        filename as filename,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp as updt_dttm
    from source
),
final as
(
    select 
        year::varchar(20) as year,
        month::varchar(20) as month,    
        store::varchar(20) as store,
        department::varchar(30) as department,
        supplier_code::varchar(50) as supplier_code,
        supplier_name::varchar(60) as supplier_name,
        item::varchar(30) as item,
        item_name::varchar(200) as item_name,
        sales_quantity::number(18,0) as sales_quantity,
        sales_amount::number(20,5) as sales_amount,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    FROM transformed
)
select * from final