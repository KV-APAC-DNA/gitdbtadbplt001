with 

source as (

    select * from {{ source('bwa_access', 'bwa_material_plant_text') }}

),

final as (

    select
        plant,
        mat_plant,
        langu,
        iff(left(trim(txtmd),1) = '"' and right(trim(txtmd),1) = '"' ,
        replace(substring(trim(txtmd),2,length(trim(txtmd))-2),'""','"'),
        replace(trim(txtmd),'""','"')) as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source
    where _deleted_='F'

)

select * from final
