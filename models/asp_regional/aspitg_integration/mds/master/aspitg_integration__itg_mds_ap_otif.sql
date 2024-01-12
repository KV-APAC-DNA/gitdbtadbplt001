--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_otif') }}
),
--Logical CTE

--Final CTE
final as (
    select
        id::number(18,0) as id,
        code::varchar(500) as code,
        year::number(31,0) as year,
        month::number(31,0) as month,
        numerator::number(31,2) as numerator,
        denominator::number(31,2) as denominator,
        market::varchar(40) as market,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

--Final select
select * from final

