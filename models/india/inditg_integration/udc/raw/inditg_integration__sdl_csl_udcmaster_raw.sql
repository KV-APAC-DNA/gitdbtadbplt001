{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_csl_udcmaster') }} 
),
final as(
    select 
        udcmasterid as udcmasterid,
        masterid as masterid,
        mastername as mastername,
        columnname as columnname,
        columndatatype as columndatatype,
        columnsize as columnsize,
        columnprecision as columnprecision,
        editable as editable,
        columnmandatory as columnmandatory,
        pickfromdefault as pickfromdefault,
        downloadstatus as downloadstatus,
        createduserid as createduserid,
        createddate as createddate,
        modifieduserid as modifieduserid,
        modifieddate as modifieddate,
        udcstatus as udcstatus,
        run_id as run_id,
        crt_dttm as crt_dttm,
        file_name as file_name
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final