with source as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_udcmaster')}}
),
final as 
(
    select
        udcmasterid::number(18,0) as udcmasterid,
        masterid::number(18,0) as masterid,
        mastername::varchar(100) as mastername,
        columnname::varchar(100) as columnname,
        columndatatype::varchar(50) as columndatatype,
        columnsize::number(18,0) as columnsize,
        columnprecision::number(18,0) as columnprecision,
        editable::number(18,0) as editable,
        columnmandatory::number(18,0) as columnmandatory,
        pickfromdefault::number(18,0) as pickfromdefault,
        downloadstatus::varchar(5) as downloadstatus,
        createduserid::number(18,0) as createduserid,
        createddate::timestamp_ntz(9) as createddate,
        modifieduserid::varchar(50) as modifieduserid,
        modifieddate::timestamp_ntz(9) as modifieddate,
        udcstatus::number(18,0) as udcstatus,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final