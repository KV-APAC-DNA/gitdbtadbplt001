{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','recdate','whcode','productcode']
    )
}}

with source as(
        SELECT *,
            dense_rank() OVER (
                PARTITION BY distributorid,recdate,whcode,productcode
                ORDER BY SOURCE_FILE_NAME DESC
                ) AS rnk
        FROM {{ source('thasdl_raw', 'sdl_th_dms_inventory_fact') }} source
        WHERE SOURCE_FILE_NAME NOT IN (
                SELECT DISTINCT file_name
                FROM {{ source('thawks_integration', 'TRATBL_sdl_th_dms_inventory_fact__test_date_format') }}
                ) qualify rnk = 1
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
        run_id::number(18,0) as run_id,
        SOURCE_FILE_NAME as file_name
    from source
)
select * from final