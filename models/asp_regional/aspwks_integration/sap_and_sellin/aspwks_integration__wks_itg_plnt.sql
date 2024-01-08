with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_plant_attr') }}
),

final as (
    select
        plant,
        country,
        logsys,
        purch_org,
        region,
        comp_code,
        factcal_id,
        zmarclust,
        updt_dttm
    from source

)

select * from final
