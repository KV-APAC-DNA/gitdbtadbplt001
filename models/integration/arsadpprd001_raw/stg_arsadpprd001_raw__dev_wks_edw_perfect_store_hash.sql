with 

source as (

    select * from {{ source('arsadpprd001_raw', 'DEV_wks_edw_perfect_store_hash') }}

),

renamed as (

    select *

    from source

)

select * from renamed
