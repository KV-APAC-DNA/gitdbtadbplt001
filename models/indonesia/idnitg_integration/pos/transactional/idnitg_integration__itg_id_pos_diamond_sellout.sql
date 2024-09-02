{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select *, dense_rank() over(partition by null order by filename desc) as rnk
    from {{ source('idnsdl_raw', 'sdl_id_pos_diamond_sellout') }}
    where filename not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_diamond_sellout__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_diamond_sellout__duplicate_test') }}
			union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_diamond_sellout__date_format_test') }}
    ) qualify rnk =1

),
final as
(
    select
    nama_barang::varchar(200) as nama_barang,
    qty::number(18,2) as qty,
    sales::number(18,2) as sales,
    pos_cust::varchar(50) as pos_cust,
    branch::varchar(100) as branch,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final