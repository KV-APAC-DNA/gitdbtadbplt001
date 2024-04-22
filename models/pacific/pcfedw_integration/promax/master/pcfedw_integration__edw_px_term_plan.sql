with source as(
    select * from {{ ref('pcfitg_integration__itg_px_term_plan') }}
),
final as(
    select 
        ac_code::varchar(50) as ac_code,
        ac_attribute::varchar(20) as ac_attribute,
        ac_longname::varchar(40) as ac_longname,
        sku_stockcode::varchar(18) as sku_stockcode,
        sku_attribute::varchar(20) as sku_attribute,
        sku_longname::varchar(40) as sku_longname,
        gltt_longname::varchar(40) as gltt_longname,
        substring(bd_shortname,6,4)::varchar(10) as bd_shortname,
        bd_longname::varchar(40) as bd_longname,
        asps_rowid::number(18,0) as asps_rowid,
        '0'::varchar(30) as asps_pubid,
        asps_type::number(38,0) as asps_type,
        asps_month1::float as asps_month1,
        asps_month2::float as asps_month2,
        asps_month3::float as asps_month3,
        asps_month4::float as asps_month4,
        asps_month5::float as asps_month5,
        asps_month6::float as asps_month6,
        asps_month7::float as asps_month7,
        asps_month8::float as asps_month8,
        asps_month9::float as asps_month9,
        asps_month10::float as asps_month10,
        asps_month11::float as asps_month11,
        asps_month12::float as asps_month12,
        gltt_rowid::number(18,0) as gltt_rowid
    from source
)
select * from final