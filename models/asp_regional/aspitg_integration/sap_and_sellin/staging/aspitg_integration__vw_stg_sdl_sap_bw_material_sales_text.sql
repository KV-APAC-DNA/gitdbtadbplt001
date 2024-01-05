with 

source as (

    select * from {{ source('bwa_access', 'bwa_material_sales_text') }}

),

final as (

    select
        salesorg,
        distr_chan,
        mat_sales,
        langu,
        txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
