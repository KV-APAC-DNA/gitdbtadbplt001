{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','recdate','whcode','productcode']
    )
}}

with source as(
    select * , dense_rank() over(partition by distributorid,recdate,whcode,productcode order by filename desc) as rnk 
    from {{ source('thasdl_raw', 'sdl_la_gt_inventory_fact') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_inventory_fact__test_format_recdate') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_inventory_fact__test_format_expirydate') }}
            ) qualify rnk =1
),
final as
(
    select
        try_to_date(recdate, 'yyyy/mm/dd') as recdate,
        distributorid::varchar(200) as distributorid,
        whcode::varchar(200) as whcode,
        productcode::varchar(200) as productcode,
        qty::number(10,4) as qty,
        amount::number(15,4) as amount,
        batchno::varchar(200) as batchno,
        try_to_date(expirydate,'yyyymmdd') as expirydate,
        filename::varchar(50) as filename,
        run_id::varchar(14) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
)
select * from final