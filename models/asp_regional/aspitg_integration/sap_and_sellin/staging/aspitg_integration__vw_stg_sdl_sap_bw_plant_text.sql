with 

source as (

    select * from {{ source('bwa_access', 'bwa_plant_text') }}

),

final as (

    select
        plant as plant,
        coalesce(txtmd, '') as txtmd,
        coalesce(txtlg, '') as txtlg,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
