with source as
(
    select * from {{ source('snapindsdl_raw', 'sdl_xdm_product') }}
),

final as 
(
    select prodcompany::varchar(10) as prodcompany,
    productcode::varchar(50) as productcode,
    productuom::varchar(5) as productuom,
    uomconvfactor::number(18,0) as uomconvfactor,
    prodhierarchylvl::varchar(50) as prodhierarchylvl,
    prodhierarchyval::varchar(50) as prodhierarchyval,
    productname::varchar(400) as productname,
    prodshortname::varchar(400) as prodshortname,
    productcmpcode::varchar(50) as productcmpcode,
    stockcovdays::number(18,0) as stockcovdays,
    productweight::number(9,3) as productweight,
    productunit::varchar(5) as productunit,
    productstatus::varchar(1) as productstatus,
    productdrugtype::varchar(1) as productdrugtype,
    serialno::varchar(1) as serialno,
    shelflife::number(18,0) as shelflife,
    franchisecode::varchar(50) as franchisecode,
    franchisename::varchar(100) as franchisename,
    brandcode::varchar(50) as brandcode,
    brandname::varchar(100) as brandname,
    product_code::varchar(50) as product_code,
    product_name::varchar(100) as product_name,
    variantcode::varchar(50) as variantcode,
    variantname::varchar(100) as variantname,
    motherskucode::varchar(50) as motherskucode,
    motherskuname::varchar(100) as motherskuname,
    eanno::varchar(50) as eanno,
    createddt::timestamp_ntz(9) as createddt,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz as crt_dttm,
    current_timestamp::timestamp_ntz as updt_dttm
    from source
)

select * from final