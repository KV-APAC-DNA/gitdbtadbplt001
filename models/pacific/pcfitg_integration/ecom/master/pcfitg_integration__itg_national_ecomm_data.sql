{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['week_end_dt'],
        pre_hook= "delete from {{this}} where week_end_dt in ( select week_end from {{ source('pcfsdl_raw', 'sdl_national_ecomm_data') }} );"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_national_ecomm_data') }}
),
final as(
    select
        pfc::varchar(20) as product_probe_id,
        skuname::varchar(100) as product_name,
        nec1_desc::varchar(100) as nec1_desc,
        nec2_desc::varchar(100) as nec2_desc,
        nec3_desc::varchar(100) as nec3_desc,
        brand::varchar(50) as brand,
        category::varchar(50) as category,
        owner as owner,
        manufacturer::varchar(50) as manufacturer,
        mat_year::varchar(10) as mat_year,
        periodid::varchar(10) as time_period,
        week_end as week_end_dt,
        cast(sales_online as decimal(10, 2)) as sales_value,
        cast(unit_online as decimal(10, 2)) as sales_qty,
        'AUD'::varchar(3) as crncy,
        file_name::varchar(50) as file_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final