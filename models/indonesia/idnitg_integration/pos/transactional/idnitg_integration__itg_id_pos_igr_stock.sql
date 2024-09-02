{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as (
    select * ,dense_rank() over(partition by null order by filename desc) as rnk 
    from {{ source('idnsdl_raw', 'sdl_id_pos_igr_stock') }}
    where filename not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_igr_stock__null_test') }}
    ) qualify rnk =1

),

final as (
    select
        item::varchar(200) as item,
        branch::varchar(50) as branch,
        qty::number(18, 2) as qty,
        unit::varchar(10) as unit,
        upper(pos_cust)::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14, 0) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
