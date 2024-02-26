with 

source as (

    select * from {{ source('bwa_access', 'bwa_material_sales_text') }}

),

final as (

    select
        coalesce(salesorg, '') as salesorg,
        coalesce(distr_chan,'') as distr_chan,
        coalesce(mat_sales, '') as mat_sales,
        coalesce(langu, '') as langu,
        iff(left(trim(txtmd),1) = '"' and right(trim(txtmd),1) = '"',
        coalesce(replace(substring(trim(txtmd),2,length(trim(txtmd))-2),'""','"'),''),
        coalesce(replace(trim(txtmd),'""','"'), '')) as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source
    where _deleted_='F'

)

select * from final
