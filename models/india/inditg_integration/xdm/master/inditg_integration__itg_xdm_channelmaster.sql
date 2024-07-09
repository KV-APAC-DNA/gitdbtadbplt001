with source as
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_channelmaster') }}
),
final as
(
    SELECT 
        cmpcode::varchar(50) as cmpcode,
        channelcode::varchar(100) as channelcode,
        channelname::varchar(100) as channelname,
        moddt::timestamp_ntz(9) as moddt,
        createddt::timestamp_ntz(9) as createddt,
        clicktype::varchar(10) as clicktype,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) AS updt_dttm
    from source
)
select * from final