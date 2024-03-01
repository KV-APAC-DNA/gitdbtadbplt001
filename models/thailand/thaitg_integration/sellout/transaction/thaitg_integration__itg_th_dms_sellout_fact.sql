{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','orderno','orderdate','arcode','linenumber']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_dms_sellout_fact') }}
),
final as(
    select
        distributorid::varchar(10) as distributorid,
        orderno::varchar(255) as orderno,
        orderdate::timestamp_ntz(9) as orderdate,
        arcode::varchar(20) as arcode,
        arname::varchar(500) as arname,
        city::varchar(255) as city,
        region::varchar(20) as region,
        saledistrict::varchar(200) as saledistrict,
        saleoffice::varchar(255) as saleoffice,
        salegroup::varchar(255) as salegroup,
        artypecode::varchar(20) as artypecode,
        saleemployee::varchar(255) as saleemployee,
        salename::varchar(350) as salename,
        productcode::varchar(25) as productcode,
        productdesc::varchar(300) as productdesc,
        megabrand::varchar(10) as megabrand,
        brand::varchar(10) as brand,
        baseproduct::varchar(20) as baseproduct,
        variant::varchar(10) as variant,
        putup::varchar(10) as putup,
        grossprice::number(19,6) as grossprice,
        qty::number(19,6) as qty,
        subamt1::number(19,6) as subamt1,
        discount::number(19,6) as discount,
        subamt2::number(19,6) as subamt2,
        discountbtline::number(19,6) as discountbtline,
        totalbeforevat::number(19,6) as totalbeforevat,
        total::number(19,6) as total,
        linenumber::number(18,0) as linenumber,
        iscancel::number(18,0) as iscancel,
        cndocno::varchar(255) as cndocno,
        cnreasoncode::varchar(255) as cnreasoncode,
        promotionheader1::varchar(255) as promotionheader1,
        promotionheader2::varchar(255) as promotionheader2,
        promotionheader3::varchar(255) as promotionheader3,
        promodesc1::varchar(255) as promodesc1,
        promodesc2::varchar(255) as promodesc2,
        promodesc3::varchar(255) as promodesc3,
        promocode1::varchar(255) as promocode1,
        promocode2::varchar(255) as promocode2,
        promocode3::varchar(255) as promocode3,
        avgdiscount::number(18,4) as avgdiscount,
        current_timestamp()::timestamp_ntz(9) as curr_date,
        run_id::number(18,0) as run_id
    from source
)
select * from final