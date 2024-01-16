--Import CTE
with source as (
    select *
    from {{ source('aspsdl_raw', 'sdl_mds_rg_tih_distributor_closure') }}
),

--Logical CTE

--Final CTE
final as (
    select
        name::varchar(500) as dstrbtr_grp_cd,
        market::varchar(200) as market,
        closemonth::number(31,0) as closemonth,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

--Final select
select * from final
