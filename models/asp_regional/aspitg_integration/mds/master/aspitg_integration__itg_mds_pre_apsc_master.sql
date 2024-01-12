--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_pre_apsc_master') }}
),

--Logical CTE

--Final CTE
final as (
    select
        year_code::varchar(500) as year_code,
        month_code::varchar(500) as month_code,
        market_code::varchar(500) as market_code,
        materialnumber::varchar(200) as materialnumber,
        materialdescription::varchar(200) as materialdescription,
        "pre-apsc per pc"::number(28,4) as pre_apsc_per_pc,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

--Final select
select * from final
