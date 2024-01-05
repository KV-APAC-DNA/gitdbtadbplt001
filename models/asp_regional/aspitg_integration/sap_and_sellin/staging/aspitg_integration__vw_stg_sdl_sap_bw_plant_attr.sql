with 

source as (

    select * from {{ source('bwa_access', 'bwa_plant_attr') }}

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
        bic_zmarclust as zmarclust,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
