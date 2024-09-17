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
    from {{ source('idnsdl_raw', 'sdl_id_pos_carrefour_sellout') }}
    where filename not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_carrefour_sellout__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_carrefour_sellout__duplicate_test') }}
			union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_carrefour_sellout__date_format_test') }}
    ) qualify rnk =1
),
final as
(
    select
    fdesc::varchar(100) as fdesc,
    scc::varchar(50) as scc,
    scc_name::varchar(200) as scc_name,
    sales_qty::number(18,2) as sales_qty,
    net_sales::number(18,2) as net_sales,
    share::number(10,2) as share,
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final