with source as
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_geohierarchy') }}
),

final as 
(
    select statecode::varchar(50) as statecode,
    statename::varchar(100) as statename,
    districtcode::varchar(50) as districtcode,
    districtname::varchar(100) as districtname,
    thesilcode::varchar(50) as thesilcode,
    thesilname::varchar(100) as thesilname,
    citycode::varchar(50) as citycode,
    cityname::varchar(100) as cityname,
    distributorcode::varchar(50) as distributorcode,
    distributorname::varchar(100) as distributorname,
    createddt::timestamp_ntz(9) as createddt,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz as crt_dttm,
    current_timestamp()::timestamp_ntz AS updt_dttm
    from source
)

select * from final