--import CTE

with sources as (
    select *
    from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_sales_office_text') }}
),

--logical CTE
final as (
    select
        mandt,
        spras,
        vkbur,
        bezei,
        --crt_dttm,
        updt_dttm
    from sources
)

--final select
select * from final
