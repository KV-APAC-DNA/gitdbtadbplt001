with source as
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_businesscalendar') }}
),
final as 
(
    select to_date(salinvdate)::timestamp_ntz(9) as salinvdate,
    month::varchar(50) as month,
    year::number(18,0) as year,
    week::varchar(50) as week,
    monthkey::number(18,0) as monthkey,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz as updt_dttm
    from source
)
select * from final