with source as
(
    select * from {{ source('snapindsdl_raw', 'sdl_xdm_productuom') }}
),

final as 
(
    select cmpcode::varchar(10) as cmpcode,
    prodcode::varchar(50) as prodcode,
    uomcode::varchar(5) as uomcode,
    uomconvfactor::number(18,0) as uomconvfactor,
    modusercode::varchar(50) as modusercode,
    moddt::timestamp_ntz(9) as moddt,
    createddt::timestamp_ntz(9) as createddt,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz as crt_dttm,
    current_timestamp()::timestamp_ntz as updt_dttm
    from source
)

select * from final