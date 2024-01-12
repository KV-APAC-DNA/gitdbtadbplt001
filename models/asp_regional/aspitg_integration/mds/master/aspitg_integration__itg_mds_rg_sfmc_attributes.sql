--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_sfmc_attributes') }}
),

--Logical CTE

--Final CTE
final as (
    select
        subject::varchar(200) as subject,
        original_value::varchar(200) as original_value,
        mapped_value::varchar(200) as mapped_value,
        market::varchar(200) as market,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

--Final select
select * from final
