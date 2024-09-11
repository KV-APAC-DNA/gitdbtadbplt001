{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','recdate','whcode','productcode']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_dms_inventory_fact') }}
),
final as(
    select
        case 
        when recdate LIKE '%/%' THEN TO_CHAR(TO_DATE(recdate, 'YYYY/MM/DD'), 'YYYY-MM-DD')
        else recdate
        end as recdate,
        distributorid::varchar(10) as distributorid,
        whcode::varchar(20) as whcode,
        productcode::varchar(25) as productcode,
        cast(qty as decimal(19, 6)) as qty,
        cast(amount as decimal(19, 6)) as amount,
        batchno::varchar(200) as batchno,
        to_date(expirydate, 'YYYYMMDD')::timestamp_ntz(9) as expirydate,
        current_timestamp()::timestamp_ntz(9) as curr_date,
        run_id::number(18,0) as run_id
    from source
)
select * from final