--Import CTE
with source as (
    select *
    from {{ source('aspsdl_raw', 'sdl_mds_rg_ecom_digital_salesweight') }}
),

--Logical CTE

--Final CTE
final as (
    select
        market::varchar(200) as market,
        eretailer::varchar(200) as eretailer,
        salesweight::varchar(200) as salesweight
    from source
)

--Final select
select * from final
