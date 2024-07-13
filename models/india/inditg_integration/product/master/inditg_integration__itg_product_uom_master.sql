with source as
(
    select * from {{ source('indsdl_raw', 'sdl_product_uom_master') }}
),
final as
(
    select 
    ltrim(material,0) as material,
    mat_unit,
    objvers,
    changed,
    denomintr,
    eanupc,
    ean_numtyp,
    gross_wt,
    height,
    len,
    numerator,
    unit,
    unit_dim,
    unit_of_wt,
    volume,
    volumeunit,
    width,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    convert_timezone('UTC',current_timestamp()) as updt_dttm 
    from source
)
select material::varchar(18) as material,
    mat_unit::varchar(3) as mat_unit,
    objvers::varchar(1) as objvers,
    changed::varchar(1) as changed,
    denomintr::number(17,3) as denomintr,
    eanupc::varchar(18) as eanupc,
    ean_numtyp::varchar(2) as ean_numtyp,
    gross_wt::number(17,3) as gross_wt,
    height::number(17,3) as height,
    len::number(17,3) as len,
    numerator::number(17,3) as numerator,
    unit::varchar(3) as unit,
    unit_dim::varchar(3) as unit_dim,
    unit_of_wt::varchar(3) as unit_of_wt,
    volume::number(17,3) as volume,
    volumeunit::varchar(3) as volumeunit,
    width::number(17,3) as width,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final