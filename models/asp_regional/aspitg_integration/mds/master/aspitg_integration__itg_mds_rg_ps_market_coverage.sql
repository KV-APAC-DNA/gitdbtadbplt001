--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_ps_market_coverage') }}
),

--Logical CTE

--Final CTE
final as (
    select
        name::varchar(255) as cnty_nm,
        channel::varchar(25) as channel,
        coverage::number(20,4) as coverage,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)



--Final select
select * from final

