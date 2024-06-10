with source as 
(
    select * from {{ source('snapindsdl_raw', 'sdl_xdm_distributor_supplier') }}
),

final as
(
    SELECT cmpcode::varchar(10) as cmpcode,
        distrcode::varchar(50) as distrcode,
        supcode::varchar(30) as supcode,
        coalesce(isdefault,'Y')::varchar(1) as isdefault,
        modusercode::varchar(50) as modusercode,
        moddt::timestamp_ntz(9) as moddt,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz AS updt_dttm
    FROM source
)

select * from final