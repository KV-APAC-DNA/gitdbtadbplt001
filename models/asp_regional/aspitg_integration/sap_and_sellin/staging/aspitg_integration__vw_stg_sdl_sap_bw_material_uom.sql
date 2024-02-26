with source as(
    select * from {{ source('bwa_access', 'bwa_material_uom') }}
),
final as(
    select
        iff(replace(material,'\"','')='',null,replace(material,'\"',''))  as material,
        iff(replace(unit,'\"','')='',null,replace(unit,'\"',''))  as unit,
        iff(replace(base_uom,'\"','')='',null,replace(base_uom,'\"',''))  as base_uom,
        iff(replace(recordmode,'\"','')='',null,replace(recordmode,'\"',''))  as recordmode,
        iff(replace(uomz1d,'\"','')='',null,replace(uomz1d,'\"','')) as uomz1d,
        iff(replace(uomn1d,'\"','')='',null,replace(uomn1d,'\"','')) as uomn1d,
        null as file_name,
        _ingestiontimestamp_ as cdl_dttm,
        _CREATETIME_::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'
)
select * from final