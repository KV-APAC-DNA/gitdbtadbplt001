with sdl_csl_tbl_schemewise_apno as
(
    select * from {{ source('indsdl_raw', 'sdl_csl_tbl_schemewise_apno') }}
),
final as
(
    SELECT 
        schid::number(18,0) as schid,
        apno::varchar(200) as apno,
        createduserid::number(18,0) as createduserid,
        createddate::timestamp_ntz(9) as createddate,
        schcategorytype1code::varchar(50) as schcategorytype1code,
        schcategorytype2code::varchar(50) as schcategorytype2code,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM sdl_csl_tbl_schemewise_apno
)
select * from final