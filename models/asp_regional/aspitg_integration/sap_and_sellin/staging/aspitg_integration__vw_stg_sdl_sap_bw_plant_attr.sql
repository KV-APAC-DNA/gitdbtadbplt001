with 

source as (

    select * from {{ source('bwa_access', 'bwa_plant_attr') }}

),

final as (

    select
        coalesce(trim(plant), '') as plant,
        coalesce(country , '') as country,
        coalesce(logsys, '') as logsys,
        coalesce(purch_org, '') as purch_org,
        coalesce(region, '') as region,
        coalesce(comp_code, '') as comp_code,
        coalesce(factcal_id, '') as factcal_id,
        coalesce(bic_zmarclust , '') as zmarclust,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source
    where _deleted_='F'

)

select * from final
