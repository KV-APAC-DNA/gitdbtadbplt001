with 

source as (

    select * from {{ source('arsadpprd001', 'WKS_EDW_PERFECT_STORE_HASH') }}

),

renamed as (

    select

    from source

)

select * from renamed