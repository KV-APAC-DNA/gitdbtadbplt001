{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','recdate','whcode','productcode']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_la_gt_inventory_fact') }}
),
final as
(
    select
        to_date(recdate, 'yyyy/mm/dd') as recdate,
        distributorid::varchar(200) as distributorid,
        whcode::varchar(200) as whcode,
        productcode::varchar(200) as productcode,
        qty::number(10,4) as qty,
        amount::number(15,4) as amount,
        batchno::varchar(200) as batchno,
        to_date(expirydate,'yyyymmdd') as expirydate,
        filename::varchar(50) as filename,
        run_id::varchar(14) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
)
select * from final