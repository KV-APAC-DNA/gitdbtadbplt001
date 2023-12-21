--import CTE

with sources as (
    select *
    from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_strongholds_text') }}
),

--logical CTE
final as (
    select
        zstrong,
        langu,
        txtsh,
        txtmd,
        --crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from sources
)

--final select
select * from final
