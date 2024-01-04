with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_plant_attr') }}

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
        crt_dttm,
        updt_dttm

    from source

)

select * from final
